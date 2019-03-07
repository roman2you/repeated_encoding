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
package com.icongtai.zebra.encoding.impl.deltastrings;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.deltalengthbytearray.DeltaLengthByteArrayValuesReader;

import java.io.IOException;


/**
 * Reads binary data written by {@link DeltaByteArrayWriter}
 * 
 */
public class DeltaByteArrayReader extends ValuesReader {
  private ValuesReader prefixLengthReader;
  private ValuesReader suffixReader;

  private Binary previous;

  public DeltaByteArrayReader() {
    this.prefixLengthReader = new DeltaIntBitPackingValuesReader();
    this.suffixReader = new DeltaLengthByteArrayValuesReader();
    this.previous = Binary.fromByteArray(new byte[0]);
  }

  @Override
  public void init(int valueCount, byte[] page, int offset)
      throws IOException {
    prefixLengthReader.init(valueCount, page, offset);
    int next = prefixLengthReader.getNextOffset();
    suffixReader.init(valueCount, page, next);
  }

  @Override
  public void skip() {
    prefixLengthReader.skip();
    suffixReader.skip();
  }

  @Override
  public Binary readBytes() {
    int prefixLength = prefixLengthReader.readInteger();
    // This does not copy bytes
    Binary suffix = suffixReader.readBytes();
    int length = prefixLength + suffix.length();
    
    // We have to do this to materialize the output
    if(prefixLength != 0) {
      byte[] out = new byte[length];
      System.arraycopy(previous.getBytes(), 0, out, 0, prefixLength);
      System.arraycopy(suffix.getBytes(), 0, out, prefixLength, suffix.length());
      previous =  Binary.fromByteArray(out);
    } else {
      previous = suffix;
    }
    return previous;
  }
}
