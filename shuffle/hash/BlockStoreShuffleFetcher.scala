
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.hash


import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator

private[hash] object BlockStoreShuffleFetcher extends Logging {
  def fetch[T](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer)
    : Iterator[T] =
  {
    /**
     * 有一种比较复杂的情况是块本来在a节点，被预调度到b，则a，b上都有块；但是正式分配的时候又被调度到a，实际上就不需要再去远端获取相应块，这里要处理一下。
     */
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    var  blocksByAddress:Seq[(BlockManagerId,Seq[(BlockId,Long)])] = null
    val blockManager = SparkEnv.get.blockManager
    val blockManagerId = blockManager.blockManagerId

    val startTime = System.currentTimeMillis

    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    val info = SparkEnv.get.mapOutputTracker.getPreFetchStatuses(shuffleId,reduceId)
    
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

      //status的所有块到地址的映射
    val allBlockToLoc = new HashMap[(BlockId,Long),BlockManagerId]
    for (((address, size), index) <- statuses.zipWithIndex) {
      allBlockToLoc.put((ShuffleBlockId(shuffleId, index, reduceId), size), address)
    }

    blocksByAddress = allBlockToLoc.toSeq.groupBy(_._2).map{case (a,b) => (a,b.map(_._1))}.toSeq


    //info等于空的话不用更新
    if(info != null) {
      val updatedBlockToLoc = new HashMap[(BlockId, Long), BlockManagerId]

        /**
         *  pre所有块到地址的映射
         *  pre块和shuffle块不一样，因此需要进行一次转换才能进行比较
         *  为了处理方便，用mapId做索引检索preFetch结果即可
         */
      val infoLoc = info.loc
      val preBlocksToAddress = new HashMap[Int, BlockManagerId]
      for ((block, size) <- info.shuffleBlockIds.zip(info.blockSizes)) {
        preBlocksToAddress.put(block.asInstanceOf[ShufflePreBlockId].mapId, infoLoc)
      }




      //开始更新
      for (((blockId,size), loc) <- allBlockToLoc) {
        /**
         * 本地块不更新；非预取块不更新
         * 先查看mapStatus中的块的位置，如果不是本地，说明块需要从其它executor取来；这时候查看预取的块是否
         * 包含该块，包含的话就从预取的地方取
         */

        val mapId = blockId.asInstanceOf[ShuffleBlockId].mapId
        if (loc.host != blockManagerId.host && preBlocksToAddress.contains(mapId)) {
          updatedBlockToLoc.put((ShufflePreBlockId(shuffleId,mapId,reduceId),size), infoLoc)
        } else {
          updatedBlockToLoc.put((blockId,size), loc)
        }
      }

      blocksByAddress =
        updatedBlockToLoc.toSeq.groupBy(_._2).map{case (a,b) => (a,b.map(_._1))}.toSeq

      if(infoLoc == blockManager.blockManagerId){
        logInfo("%%%%%% preFetch blocks and used is " + info.shuffleBlockIds.length + " %%%%%%")
      }
    }


    def unpackBlock(blockPair: (BlockId, Try[Iterator[Any]])) : Iterator[T] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Success(block) => {
          block.asInstanceOf[Iterator[T]]
        }
        case Failure(e) => {
          blockId match {
            case ShuffleBlockId(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
            case ShufflePreBlockId(shufId, mapId, _) =>
              val address = info.loc
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block", e)
          }
        }
      }
    }

    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      serializer,
      SparkEnv.get.conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)
    val itr = blockFetcherItr.flatMap(unpackBlock)

    val completionIter = CompletionIterator[T, Iterator[T]](itr, {
      context.taskMetrics.updateShuffleReadMetrics()
    })

    new InterruptibleIterator[T](context, completionIter) {
      val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
      override def next(): T = {
        readMetrics.incRecordsRead(1)
        delegate.next()
      }
    }
  }
}
