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

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;


/**
 * and stores the values in an in memory buffer, which is less than ideal.
 *
 */
public class RunLengthBitPackingHybridValuesReader extends ValuesReader {
  private RunLengthBitPackingHybridDecoder decoder;
  private int nextOffset;
  private boolean longFlag;

  public RunLengthBitPackingHybridValuesReader(boolean longFlag) {
    this.longFlag = longFlag;
  }

  @Override
  public void init(int valueCount, byte[] buf, int offset) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(buf, offset, buf.length - offset);
    int length = BytesUtils.readIntLittleEndian(in);
    if(longFlag) {
      decoder = RunLengthBitPackingHybridDecoder.newLongDecoder(in);
    } else {
      decoder = RunLengthBitPackingHybridDecoder.newIntDecoder(in);
    }


    // 4 is for the length which is stored as 4 bytes little endian
    this.nextOffset = offset + length + 4;
  }

  @Override
  public int getNextOffset() {
    return this.nextOffset;
  }

  @Override
  public int readInteger() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public long readLong() {
    try {
      return decoder.readLong();
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  public int getCurrentCount() {
    return decoder.getCurrentCount();
  }


  @Override
  public boolean readBoolean() {
    return readInteger() == 0 ? false : true;
  }

  @Override
  public void skip() {
    if(longFlag) {
      readLong();
    } else {
      readInteger();
    }
  }
}
