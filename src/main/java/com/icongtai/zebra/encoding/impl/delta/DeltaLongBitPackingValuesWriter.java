/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.icongtai.zebra.encoding.impl.delta;


import com.icongtai.zebra.encoding.bitpacking.BytePackerForLong;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.CapacityByteArrayOutputStream;

/**
 *
 * The algorithm and format is inspired by D. Lemire's paper: http://lemire.me/blog/archives/2012/09/12/fast-integer-compression-decoding-billions-of-integers-per-second/
 *
 */
public class DeltaLongBitPackingValuesWriter extends DeltaIntBitPackingValuesWriter {


  /**
   * stores delta values starting from the 2nd value written(1st value is stored in header).
   * It's reused between flushes
   */
  private long[] deltaBlockBuffer;


  /**
   * firstValue is written to the header of the page
   */
  private long firstValue = 0;

  /**
   * cache previous written value for calculating delta
   */
  private long previousValue = 0;

  public DeltaLongBitPackingValuesWriter(int initSlabSize) {
    this(DEFAULT_NUM_BLOCK_VALUES, DEFAULT_NUM_MINIBLOCKS, initSlabSize);
  }

  public DeltaLongBitPackingValuesWriter(int blockSizeInValues, int miniBlockNum, int initSlabSize) {
    this.config = new DeltaBitPackingConfig(blockSizeInValues, miniBlockNum);
    this.bitWidths = new int[config.miniBlockNumInABlock];
    this.deltaBlockBuffer = new long[blockSizeInValues];
    this.miniBlockByteBuffer = new byte[config.miniBlockSizeInValues * MAX_BITWIDTH];
    this.baos = new CapacityByteArrayOutputStream(initSlabSize);
  }


  @Override
  public void writeLong(long v) {
    totalValueCount++;

    if (totalValueCount == 1) {
      firstValue = v;
      previousValue = firstValue;
      return;
    }

    long delta = v - previousValue;//calculate delta
    previousValue = v;

    deltaBlockBuffer[deltaValuesToFlush++] = delta;

    if (delta < minDeltaInCurrentBlock) {
      minDeltaInCurrentBlock = (int) delta;
    }

    if (config.blockSizeInValues == deltaValuesToFlush) {
      flushBlockBuffer();
    }
  }

  protected void flushBlockBuffer() {
    //since we store the min delta, the deltas will be converted to be the difference to min delta and all positive
    for (int i = 0; i < deltaValuesToFlush; i++) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaInCurrentBlock;
    }

    writeMinDelta();
    int miniBlocksToFlush = getMiniBlockCountToFlush(deltaValuesToFlush);

    calculateBitWidthsForDeltaBlockBuffer(miniBlocksToFlush);
    for (int i = 0; i < config.miniBlockNumInABlock; i++) {
      writeBitWidthForMiniBlock(i);
    }

    for (int i = 0; i < miniBlocksToFlush; i++) {
      //writing i th miniblock
      int currentBitWidth = bitWidths[i];
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(currentBitWidth);
      int miniBlockStart = i * config.miniBlockSizeInValues;
      for (int j = miniBlockStart; j < (i + 1) * config.miniBlockSizeInValues; j += 8) {//8 values per pack
        // mini block is atomic in terms of flushing
        // This may write more values when reach to the end of data writing to last mini block,
        // since it may not be aligend to miniblock,
        // but doesnt matter. The reader uses total count to see if reached the end.
        packer.pack8Values(deltaBlockBuffer, j, miniBlockByteBuffer, 0);
        baos.write(miniBlockByteBuffer, 0, currentBitWidth);
      }
    }

    minDeltaInCurrentBlock = Integer.MAX_VALUE;
    deltaValuesToFlush = 0;
  }

  /**
   * iterate through values in each mini block and calculate the bitWidths of max values.
   *
   * @param miniBlocksToFlush
   */
  protected void calculateBitWidthsForDeltaBlockBuffer(int miniBlocksToFlush) {
    for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksToFlush; miniBlockIndex++) {

      int mask = 0;
      int miniStart = miniBlockIndex * config.miniBlockSizeInValues;

      //The end of current mini block could be the end of current block(deltaValuesToFlush) buffer when data is not aligned to mini block
      int miniEnd = Math.min((miniBlockIndex + 1) * config.miniBlockSizeInValues, deltaValuesToFlush);

      for (int i = miniStart; i < miniEnd; i++) {
        mask |= deltaBlockBuffer[i];
      }
      bitWidths[miniBlockIndex] = 32 - Integer.numberOfLeadingZeros(mask);
    }
  }


  /**
   * getBytes will trigger flushing block buffer, DO NOT write after getBytes() is called without calling reset()
   *
   * @return
   */
  @Override
  public BytesInput getBytes() {
    //The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
    if (deltaValuesToFlush != 0) {
      flushBlockBuffer();
    }
    return BytesInput.concat(
            config.toBytesInput(),
            BytesInput.fromUnsignedVarInt(totalValueCount),
            BytesInput.fromZigZagVarLong(firstValue),
            BytesInput.from(baos));
  }


  @Override
  public long getAllocatedSize() {
    return baos.getCapacity();
  }

}
