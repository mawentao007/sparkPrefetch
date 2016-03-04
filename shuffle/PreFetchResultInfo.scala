package org.apache.spark.shuffle

import java.io.{ObjectInput, ObjectOutput, Externalizable}


import org.apache.spark.storage.{ShuffleBlockId, BlockManagerId}
import org.apache.spark.util.Utils
import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer

/**
 * Created by marvin on 16-3-4.
 */
class PreFetchResultInfo(var loc:BlockManagerId,
                         var preFetchedBlockIds:Array[ShuffleBlockId]
                        ) extends Externalizable with Logging {

  protected def this() = this(null.asInstanceOf[BlockManagerId],-1,
      null.asInstanceOf[Array[ShuffleBlockId]])


  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    //logInfo("******** shuffleBlockInfo   writeExternal  ********")
    loc.writeExternal(out)
    out.writeInt(preFetchedBlockIds.length)
    preFetchedBlockIds.map(x => out.writeObject(x))
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val dataLen = in.readInt()
    val preFetchedBlockIdsBuffer = new ArrayBuffer[ShuffleBlockId]
    for(x <- 0 to dataLen-1){
      val tmp = in.readObject()
      preFetchedBlockIdsBuffer.append(tmp.asInstanceOf[ShuffleBlockId])
    }
    preFetchedBlockIds = preFetchedBlockIdsBuffer.toArray
  }
}



