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
package com.icongtai.zebra.encoding.impl.dictionary;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.Dictionary;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;


/**
 * Reads values that have been dictionary encoded
 *
 */
public class DictionaryValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(DictionaryValuesReader.class);

  private ByteArrayInputStream in;

  private Dictionary dictionary;

  private RunLengthBitPackingHybridDecoder decoder;

  public DictionaryValuesReader(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public void init(int valueCount, byte[] page, int offset)
      throws IOException {
    this.in = new ByteArrayInputStream(page, offset, page.length - offset);
    if (page.length - offset > 0) {
      if (LOG.DEBUG)
        LOG.debug("init from page at offset " + offset + " for length " + (page.length - offset));
      int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
      if (LOG.DEBUG) LOG.debug("bit width " + bitWidth);
      decoder = RunLengthBitPackingHybridDecoder.newIntDecoder(in);
    } else {
      decoder = RunLengthBitPackingHybridDecoder.newIntDecoder(in);
    }
  }

  @Override
  public int readValueDictionaryId() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public Binary readBytes() {
    try {
      return dictionary.decodeToBinary(decoder.readInt());
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public float readFloat() {
    try {
      return dictionary.decodeToFloat(decoder.readInt());
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return dictionary.decodeToDouble(decoder.readInt());
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public int readInteger() {
    try {
      return dictionary.decodeToInt(decoder.readInt());
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public long readLong() {
    try {
      return dictionary.decodeToLong(decoder.readInt());
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }

  @Override
  public void skip() {
    try {
      decoder.readInt(); // Type does not matter as we are just skipping dictionary keys
    } catch (IOException e) {
      throw new ZebraDecodingException(e);
    }
  }
}
