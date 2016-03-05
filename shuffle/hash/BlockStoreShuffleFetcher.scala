
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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockFetcherIterator, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

private[hash] object BlockStoreShuffleFetcher extends Logging {
  def fetch[T](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer)
    : Iterator[T] =
  {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    var  blocksByAddress:Seq[(BlockManagerId,Seq[(BlockId,Long)])] = null
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis

    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    //mv
    var preBlocksByAddress:HashMap[BlockManagerId,Array[(BlockId,Long)]] = HashMap()
    var preFetchedBlocks:Array[ShuffleBlockId] = Array()
    val info = SparkEnv.get.mapOutputTracker.getPreFetchStatuses(shuffleId,reduceId)
    if(info != null) {
      preFetchedBlocks = info.shuffleBlockIds
      preBlocksByAddress = HashMap(info.loc -> preFetchedBlocks.zip(info.blockSizes))
      blocksByAddress = preBlocksByAddress.toSeq.map {
        case (x, y) => (x, y.toSeq)
      }
    }


    //有个别块不见了
    if(preFetchedBlocks.length != statuses.length) {
      logInfo("%%%%%% some blocks not preFetch, get num is " + preFetchedBlocks.length + "%%%%%%")
      logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
        shuffleId, reduceId, System.currentTimeMillis - startTime))

      val locAndSizeByBlockId = new HashMap[BlockId, (BlockManagerId, Long)]

      for (((address, size), index) <- statuses.zipWithIndex) {
        locAndSizeByBlockId.put(ShuffleBlockId(shuffleId, index, reduceId), (address, size))
      }

      val lostBlocks = locAndSizeByBlockId.keySet.filter(!preFetchedBlocks.contains(_))
      val lostBlocksByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
      lostBlocks.toSeq.map {
        case bId =>
          val (address, size) = locAndSizeByBlockId.get(bId).get
          lostBlocksByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((bId, size))
      }


      lostBlocksByAddress.foreach{
        case (loc,v) =>
          val newv = preBlocksByAddress.getOrElse(loc,Array()) ++ v
          preBlocksByAddress.update(loc,newv)

      }

      blocksByAddress = preBlocksByAddress.toSeq.map {
        case (x, y) => (x, y.toSeq)
      }
    }
    //--mv




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
/*
package org.apache.spark.shuffle.hash

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockFetcherIterator, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

private[hash] object BlockStoreShuffleFetcher extends Logging {
  def fetch[T](
                shuffleId: Int,
                reduceId: Int,
                context: TaskContext,
                serializer: Serializer)
  : Iterator[T] =
  {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2)))
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
}*/
