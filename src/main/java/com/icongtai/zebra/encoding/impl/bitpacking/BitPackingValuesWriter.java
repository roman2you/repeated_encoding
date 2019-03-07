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
package com.icongtai.zebra.encoding.impl.bitpacking;


import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bitpacking.BitPacking;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.CapacityByteArrayOutputStream;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.EncodeType;

import java.io.IOException;

import static com.icongtai.zebra.encoding.bitpacking.BitPacking.getBitPackingWriter;
import static com.icongtai.zebra.encoding.bytes.BytesUtils.getWidthFromMaxInt;


/**
 * a column writer that packs the ints in the number of bits required based on the maximum size.
 *
 */
public class BitPackingValuesWriter extends ValuesWriter {

  private CapacityByteArrayOutputStream out;
  private BitPacking.BitPackingWriter bitPackingWriter;
  private int bitsPerValue;

  /**
   * @param bound the maximum value stored by this column
   */
  public BitPackingValuesWriter(int bound, int initialCapacity) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.out = new CapacityByteArrayOutputStream(initialCapacity);
    init();
  }



  private void init() {
    this.bitPackingWriter = getBitPackingWriter(bitsPerValue, out);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeInteger(int v) {
    try {
      bitPackingWriter.write(v);
    } catch (IOException e) {
      throw new ZebraEncodingException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getBufferedSize() {
    return out.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BytesInput getBytes() {
    try {
      this.bitPackingWriter.finish();
      return BytesInput.from(out);
    } catch (IOException e) {
      throw new ZebraEncodingException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() {
    out.reset();
    init();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getAllocatedSize() {
    return out.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return out.memUsageString(prefix);
  }

  @Override
  public EncodeType getEncoding() {
    return EncodeType.bit_packing;
  }

}
