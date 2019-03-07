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
package com.icongtai.zebra.encoding.impl.deltalengthbytearray;


import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;

import java.io.IOException;


/**
 * Reads binary data written by {@link DeltaLengthByteArrayValuesWriter}
 *
 */
public class DeltaLengthByteArrayValuesReader extends ValuesReader {

  private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesReader.class);
  private ValuesReader lengthReader;
  private byte[] in;
  private int offset;

  public DeltaLengthByteArrayValuesReader() {
    this.lengthReader = new DeltaIntBitPackingValuesReader();
  }

  @Override
  public void init(int valueCount, byte[] in, int offset)
      throws IOException {
    if (LOG.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    lengthReader.init(valueCount, in, offset);
    offset = lengthReader.getNextOffset();
    this.in = in;
    this.offset = offset;
  }

  @Override
  public Binary readBytes() {
    int length = lengthReader.readInteger();
    int start = offset;
    offset = start + length;
    return Binary.fromByteArray(in, start, length);
  }

  @Override
  public void skip() {
    int length = lengthReader.readInteger();
    offset = offset + length;
  }
}
