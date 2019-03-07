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
package com.icongtai.zebra.encoding.impl.rle;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.EncodeType;

import java.io.IOException;


/**
 * 混合了BitPacking和RLE的编码，对有大量重复的小数据非常适合这种编码，比如gps精度
 */
public class RunLengthBitPackingHybridValuesWriter extends ValuesWriter {
  private RunLengthBitPackingHybridEncoder encoder;

  public RunLengthBitPackingHybridValuesWriter(int max, int min, int initialCapacity) {
    Preconditions.checkArgument(max >= min , "max should >= min arg error");
    boolean signed = false;
    if(min < 0) {
      signed = true;
    }
    int bitWidth;
    if(!signed) {
      bitWidth = BytesUtils.getWidthFromMaxInt(max);
    } else {
      bitWidth = Math.max(BytesUtils.getWidthFromMaxInt(BytesUtils.zigzagInt(min)), BytesUtils.getWidthFromMaxInt(BytesUtils.zigzagInt(max)));
    }
    this.encoder = RunLengthBitPackingHybridEncoder.newIntEncoder(bitWidth, signed, initialCapacity);
  }

  public RunLengthBitPackingHybridValuesWriter(long max, long min, int initialCapacity) {
    Preconditions.checkArgument(max > min , "max < min arg error");
    boolean signed = false;
    if(min < 0) {
      signed = true;
    }
    int bitWidth;
    if(!signed) {
      bitWidth = BytesUtils.getWidthFromMaxLong(max);
    } else {
      bitWidth = Math.max(BytesUtils.getWidthFromMaxLong(BytesUtils.zigzagLong(min)), BytesUtils.getWidthFromMaxLong(BytesUtils.zigzagLong(max)));
    }
    this.encoder = RunLengthBitPackingHybridEncoder.newLongEncoder(bitWidth, signed, initialCapacity);
  }

  @Override
  public void writeInteger(int v) {
    try {
      encoder.writeInt(v);
    } catch (IOException e) {
      throw new ZebraEncodingException(e);
    }
  }

  @Override
  public void writeLong(long v) {
    try {
      encoder.writeLong(v);
    } catch (IOException e) {
      throw new ZebraEncodingException(e);
    }
  }

  @Override
  public void writeBoolean(boolean v) {
    writeInteger(v ? 1 : 0);
  }

  @Override
  public long getBufferedSize() {
    return encoder.getBufferedSize();
  }

  @Override
  public long getAllocatedSize() {
    return encoder.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    try {
      // prepend the length of the column
      BytesInput rle = encoder.toBytes();
      return BytesInput.concat(BytesInput.fromInt(Ints.checkedCast(rle.size())), rle);
    } catch (IOException e) {
      throw new ZebraEncodingException(e);
    }
  }

  @Override
  public EncodeType getEncoding() {
    return EncodeType.rle;
  }

  @Override
  public void reset() {
    encoder.reset();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s RunLengthBitPackingHybrid %d bytes", prefix, getAllocatedSize());
  }
}
