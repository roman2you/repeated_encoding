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


import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;

import java.io.IOException;


public class BinaryPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(BinaryPlainValuesReader.class);
  private byte[] in;
  private int offset;

  @Override
  public Binary readBytes() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      int start = offset + 4;
      offset = start + length;
      return Binary.fromByteArray(in, start, length);
    } catch (IOException e) {
      throw new ZebraDecodingException("could not read bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ZebraDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      offset += 4 + length;
    } catch (IOException e) {
      throw new ZebraDecodingException("could not skip bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ZebraDecodingException("could not skip bytes at offset " + offset, e);
    }
  }

  @Override
  public void init(int valueCount, byte[] in, int offset)
      throws IOException {
    if (LOG.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = in;
    this.offset = offset;
  }

}
