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

package org.apache.spark.shuffle

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.Logging
import org.apache.spark.storage.{ShuffleBlockId, BlockManagerId}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * Created by marvin on 15-4-21.
 */

/**
 * driver端生成并发送给executor,用来计算要获取的block的信息
 */
class ShuffleBlockInfo(
                        var loc:BlockManagerId,
                        var shuffleBlockIds:Array[ShuffleBlockId],
                        var blockSizes:Array[Long]) extends Externalizable with Logging {

  protected def this() = this(null.asInstanceOf[BlockManagerId], null.asInstanceOf[Array[ShuffleBlockId]],null.asInstanceOf[Array[Long]])


  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    //logInfo("******** shuffleBlockInfo   writeExternal  ********")
    loc.writeExternal(out)
    out.writeInt(shuffleBlockIds.length)
    shuffleBlockIds.map(x => out.writeObject(x))
    out.write(blockSizes.map(ShuffleBlockInfo.compressSize))
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val dataLen = in.readInt()
    val shuffleBlockIdsBuffer = new ArrayBuffer[ShuffleBlockId]
    for(x <- 0 to dataLen-1){
      val tmp = in.readObject()
      shuffleBlockIdsBuffer.append(tmp.asInstanceOf[ShuffleBlockId])
    }
    shuffleBlockIds = shuffleBlockIdsBuffer.toArray
    val blockSizesByte = new Array[Byte](dataLen)
    in.readFully(blockSizesByte)
    blockSizes = blockSizesByte.map(ShuffleBlockInfo.decompressSize)
  }


}



object ShuffleBlockInfo {

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  def compressSize(size: Int): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }


  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }

  def decompressSizeToInt(compressedSize: Byte): Int = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toInt
    }
  }






}
