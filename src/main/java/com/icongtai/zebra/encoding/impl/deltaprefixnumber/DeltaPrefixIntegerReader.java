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
package com.icongtai.zebra.encoding.impl.deltaprefixnumber;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesReader;

import java.io.IOException;


/**
 * Reads binary data written by {@link DeltaPrefixIntegerWriter}
 * 
 */
public class DeltaPrefixIntegerReader extends ValuesReader {
  private ValuesReader prefixLengthReader;
  private ValuesReader suffixReader;

  private int suffixLength;

  public DeltaPrefixIntegerReader(boolean longFlag, int suffixLength) {
    this.prefixLengthReader = new RunLengthBitPackingHybridValuesReader(longFlag);
    this.suffixReader = new DeltaIntBitPackingValuesReader();
    this.suffixLength = suffixLength;
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
  public int getNextOffset() {
    return suffixReader.getNextOffset();
  }


  @Override
  public int readInteger() {
    int prefix = prefixLengthReader.readInteger();
    // This does not copy bytes
    int suffix = suffixReader.readInteger();
    String suffixString = String.valueOf(suffix);
    if(suffixString.length() < suffixLength) {
      int oriSuffixLen = suffixString.length();
      for(int i = 0; i < suffixLength - oriSuffixLen; i++) {
        suffixString = "0" + suffixString;
      }
    }


    return Integer.parseInt(String.valueOf(prefix) + suffixString);
  }


  @Override
  public long readLong() {
    int prefix = prefixLengthReader.readInteger();
    // This does not copy bytes
    int suffix = suffixReader.readInteger();
    String suffixString = String.valueOf(suffix);
    if (suffixString.length() < suffixLength) {
      for (int i = 0; i < suffixLength - suffixString.length(); i++) {
        suffixString = "0" + suffixString;
      }
    }

    return Long.parseLong(String.valueOf(prefix) + suffixString);
  }
}
