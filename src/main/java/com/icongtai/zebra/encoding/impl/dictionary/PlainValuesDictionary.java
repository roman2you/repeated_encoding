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


import com.google.common.base.Preconditions;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.Dictionary;
import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.DictionaryPage;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.plain.PlainValuesReader;

import java.io.IOException;

import static com.icongtai.zebra.encoding.bytes.BytesUtils.readIntLittleEndian;


/**
 * a simple implementation of dictionary for plain encoded values
 *
 */
public abstract class PlainValuesDictionary extends Dictionary {

  /**
   * @param dictionaryPage the PLAIN encoded content of the dictionary
   * @throws IOException
   */
  protected PlainValuesDictionary(DictionaryPage dictionaryPage) throws IOException {
    super(dictionaryPage.getEncoding());
    if (dictionaryPage.getEncoding() != EncodeType.plan_dictionary && dictionaryPage.getEncoding() != EncodeType.plain) {
      throw new ZebraDecodingException("Dictionary data encoding type not supported: " + dictionaryPage.getEncoding());
    }
  }

  public static Dictionary initDictionary(ColumnType type, DictionaryPage dictionaryPage) throws IOException {
    switch (type) {
      case STRING:
        return new PlainBinaryDictionary(dictionaryPage);
//      case FIXED_LEN_BYTE_ARRAY:
//        return new PlainBinaryDictionary(dictionaryPage, descriptor.getTypeLength());
      case LONG:
      case UNSIGNED_LONG:
        return new PlainLongDictionary(dictionaryPage);
      case DOUBLE:
        return new PlainDoubleDictionary(dictionaryPage);
      case INT:
      case UNSIGNED_INT:
      case SHORT:
      case BYTE:
        return new PlainIntegerDictionary(dictionaryPage);
      case FLOAT:
        return new PlainFloatDictionary(dictionaryPage);
      default:
        throw new ZebraDecodingException("Dictionary encoding not supported for type: " + type);
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded binary
   */
  public static class PlainBinaryDictionary extends PlainValuesDictionary {

    private Binary[] binaryDictionaryContent = null;

    /**
     * Decodes {@link Binary} values from a {@link DictionaryPage}.
     *
     * Values are read as length-prefixed values with a 4-byte little-endian
     * length.
     *
     * @param dictionaryPage a {@code DictionaryPage} of encoded binary values
     * @throws IOException
     */
    public PlainBinaryDictionary(DictionaryPage dictionaryPage) throws IOException {
      this(dictionaryPage, null);
    }

    /**
     * Decodes {@link Binary} values from a {@link DictionaryPage}.
     *
     * If the given {@code length} is null, the values will be read as length-
     * prefixed values with a 4-byte little-endian length. If length is not
     * null, it will be used as the length for all fixed-length {@code Binary}
     * values read from the page.
     *
     * @param dictionaryPage a {@code DictionaryPage} of encoded binary values
     * @param length a fixed length of binary arrays, or null if not fixed
     * @throws IOException
     */
    public PlainBinaryDictionary(DictionaryPage dictionaryPage, Integer length) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      binaryDictionaryContent = new Binary[dictionaryPage.getDictionarySize()];
      int offset = 0;
      if (length == null) {
        // dictionary values are stored in order: size (4 bytes LE) followed by {size} bytes
        for (int i = 0; i < binaryDictionaryContent.length; i++) {
          int len = readIntLittleEndian(dictionaryBytes, offset);
          // read the length
          offset += 4;
          // wrap the content in a binary
          binaryDictionaryContent[i] = Binary.fromByteArray(dictionaryBytes, offset, len);
          // increment to the next value
          offset += len;
        }
      } else {
        // dictionary values are stored as fixed-length arrays
        Preconditions.checkArgument(length > 0,
                "Invalid byte array length: " + length);
        for (int i = 0; i < binaryDictionaryContent.length; i++) {
          // wrap the content in a Binary
          binaryDictionaryContent[i] = Binary.fromByteArray(
              dictionaryBytes, offset, length);
          // increment to the next value
          offset += length;
        }
      }
    }

    @Override
    public Binary decodeToBinary(int id) {
      return binaryDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainBinaryDictionary {\n");
      for (int i = 0; i < binaryDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(binaryDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return binaryDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded long values
   */
  public static class PlainLongDictionary extends PlainValuesDictionary {

    private long[] longDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainLongDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      longDictionaryContent = new long[dictionaryPage.getDictionarySize()];
      PlainValuesReader.LongPlainValuesReader longReader = new PlainValuesReader.LongPlainValuesReader();
      longReader.init(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < longDictionaryContent.length; i++) {
        longDictionaryContent[i] = longReader.readLong();
      }
    }

    @Override
    public long decodeToLong(int id) {
      return longDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainLongDictionary {\n");
      for (int i = 0; i < longDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(longDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return longDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded double values
   */
  public static class PlainDoubleDictionary extends PlainValuesDictionary {

    private double[] doubleDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainDoubleDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      doubleDictionaryContent = new double[dictionaryPage.getDictionarySize()];
      PlainValuesReader.DoublePlainValuesReader doubleReader = new PlainValuesReader.DoublePlainValuesReader();
      doubleReader.init(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        doubleDictionaryContent[i] = doubleReader.readDouble();
      }
    }

    @Override
    public double decodeToDouble(int id) {
      return doubleDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainDoubleDictionary {\n");
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(doubleDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return doubleDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded integer values
   */
  public static class PlainIntegerDictionary extends PlainValuesDictionary {

    private int[] intDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainIntegerDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      intDictionaryContent = new int[dictionaryPage.getDictionarySize()];
      PlainValuesReader.IntegerPlainValuesReader intReader = new PlainValuesReader.IntegerPlainValuesReader();
      intReader.init(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < intDictionaryContent.length; i++) {
        intDictionaryContent[i] = intReader.readInteger();
      }
    }

    @Override
    public int decodeToInt(int id) {
      return intDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainIntegerDictionary {\n");
      for (int i = 0; i < intDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(intDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return intDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded float values
   */
  public static class PlainFloatDictionary extends PlainValuesDictionary {

    private float[] floatDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainFloatDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      floatDictionaryContent = new float[dictionaryPage.getDictionarySize()];
      PlainValuesReader.FloatPlainValuesReader floatReader = new PlainValuesReader.FloatPlainValuesReader();
      floatReader.init(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        floatDictionaryContent[i] = floatReader.readFloat();
      }
    }

    @Override
    public float decodeToFloat(int id) {
      return floatDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainFloatDictionary {\n");
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(floatDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return floatDictionaryContent.length - 1;
    }

  }

}
