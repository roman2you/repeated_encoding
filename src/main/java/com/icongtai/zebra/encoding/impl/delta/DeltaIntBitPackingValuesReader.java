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



import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bitpacking.BytePacker;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Read values written by {@link DeltaIntBitPackingValuesWriter}
 *
 */
public class DeltaIntBitPackingValuesReader extends ValuesReader {
  protected int totalValueCount;
  /**
   * values read by the caller
   */
  protected int valuesRead;
  protected int minDeltaInCurrentBlock;
  protected byte[] page;
  /**
   * stores the decoded values including the first value which is written to the header
   */
  protected int[] valuesBuffer;
  /**
   * values loaded to the buffer, it could be bigger than the totalValueCount
   * when data is not aligned to mini block, which means padding 0s are in the buffer
   */
  protected int valuesBuffered;
  protected ByteArrayInputStream in;
  protected int nextOffset;
  protected DeltaBitPackingConfig config;
  protected int[] bitWidths;

  /**
   * eagerly load all the data into memory
   *
   * @param valueCount count of values in this page
   * @param page       the array to read from containing the page data (repetition levels, definition levels, data)
   * @param offset     where to start reading from in the page
   * @throws IOException
   */
  @Override
  public void init(int valueCount, byte[] page, int offset) throws IOException {
    in = new ByteArrayInputStream(page, offset, page.length - offset);
    this.config = DeltaBitPackingConfig.readConfig(in);
    this.page = page;
    this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
    allocateValuesBuffer();
    bitWidths = new int[config.miniBlockNumInABlock];

    //read first value from header
    valuesBuffer[valuesBuffered++] = BytesUtils.readZigZagVarInt(in);

    while (valuesBuffered < totalValueCount) { //values Buffered could be more than totalValueCount, since we flush on a mini block basis
      loadNewBlockToBuffer();
    }
    this.nextOffset = page.length - in.available();
  }
  
  @Override
  public int getNextOffset() {
    return nextOffset;
  }
  
  /**
   * the value buffer is allocated so that the size of it is multiple of mini block
   * because when writing, data is flushed on a mini block basis
   */
  protected void allocateValuesBuffer() {
    int totalMiniBlockCount = (int) Math.ceil((double) totalValueCount / config.miniBlockSizeInValues);
    //+ 1 because first value written to header is also stored in values buffer
    valuesBuffer = new int[totalMiniBlockCount * config.miniBlockSizeInValues + 1];
  }

  @Override
  public void skip() {
    checkRead();
    valuesRead++;
  }

  @Override
  public int readInteger() {
    checkRead();
    return valuesBuffer[valuesRead++];
  }

  private void checkRead() {
    if (valuesRead >= totalValueCount) {
      throw new ZebraDecodingException("no more value to read, total value count is " + totalValueCount);
    }
  }

  protected void loadNewBlockToBuffer() {
    try {
      minDeltaInCurrentBlock = BytesUtils.readZigZagVarInt(in);
    } catch (IOException e) {
      throw new ZebraDecodingException("can not read min delta in current block", e);
    }

    readBitWidthsForMiniBlocks();

    // mini block is atomic for reading, we read a mini block when there are more values left
    int i;
    for (i = 0; i < config.miniBlockNumInABlock && valuesBuffered < totalValueCount; i++) {
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidths[i]);
      unpackMiniBlock(packer);
    }

    //calculate values from deltas unpacked for current block
    int valueUnpacked=i*config.miniBlockSizeInValues;

    writeDeltaValues(valueUnpacked);
  }

  protected void writeDeltaValues(int valueUnpacked) {
    for (int j = valuesBuffered-valueUnpacked; j < valuesBuffered; j++) {
      int index = j;
      valuesBuffer[index] += minDeltaInCurrentBlock + valuesBuffer[index - 1];
    }
  }

  /**
   * mini block has a size of 8*n, unpack 8 value each time
   *
   * @param packer the packer created from bitwidth of current mini block
   */
  private void unpackMiniBlock(BytePacker packer) {
    for (int j = 0; j < config.miniBlockSizeInValues; j += 8) {
      unpack8Values(packer);
    }
  }

  private void unpack8Values(BytePacker packer) {
    //calculate the pos because the packer api uses array not stream
    int pos = page.length - in.available();
    packer.unpack8Values(page, pos, valuesBuffer, valuesBuffered);
    this.valuesBuffered += 8;
    //sync the pos in stream
    in.skip(packer.getBitWidth());
  }

  protected void readBitWidthsForMiniBlocks() {
    for (int i = 0; i < config.miniBlockNumInABlock; i++) {
      try {
        bitWidths[i] = BytesUtils.readIntLittleEndianOnOneByte(in);
      } catch (IOException e) {
        throw new ZebraDecodingException("Can not decode bitwidth in block header", e);
      }
    }
  }

  public int getTotalValueCount() {
    return totalValueCount;
  }
}
