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
package com.icongtai.zebra.encoding.impl.plain;


import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.bitpacking.ByteBitPackingValuesWriter;

/**
 * An implementation of the PLAIN encoding
 *
 */
public class BooleanPlainValuesWriter extends ValuesWriter {

  private ByteBitPackingValuesWriter bitPackingWriter;

  public BooleanPlainValuesWriter() {
    bitPackingWriter = new ByteBitPackingValuesWriter(1, Packer.LITTLE_ENDIAN);
  }

  @Override
  public final void writeBoolean(boolean v) {
    bitPackingWriter.writeInteger(v ? 1 : 0);
  }

  @Override
  public long getBufferedSize() {
    return bitPackingWriter.getBufferedSize();
  }

  @Override
  public BytesInput getBytes() {
    return bitPackingWriter.getBytes();
  }

  @Override
  public void reset() {
    bitPackingWriter.reset();
  }

  @Override
  public long getAllocatedSize() {
    return bitPackingWriter.getAllocatedSize();
  }

  @Override
  public EncodeType getEncoding() {
    return EncodeType.plain;
  }

  @Override
  public String memUsageString(String prefix) {
    return bitPackingWriter.memUsageString(prefix);
  }

}
