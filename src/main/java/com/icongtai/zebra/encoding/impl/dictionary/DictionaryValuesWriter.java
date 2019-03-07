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

import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.format.DictionaryPage;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.plain.FixedLenByteArrayPlainValuesWriter;
import com.icongtai.zebra.encoding.impl.plain.PlainValuesWriter;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridEncoder;
import it.unimi.dsi.fastutil.doubles.Double2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.floats.Float2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static com.icongtai.zebra.encoding.bytes.BytesInput.concat;

/**
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 *  if the dictionary gets too big
 *
 *
 */
public abstract class DictionaryValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(DictionaryValuesWriter.class);

  /* max entries allowed for the dictionary will fail over to plain encoding if reached */
  private static final int MAX_DICTIONARY_ENTRIES = Integer.MAX_VALUE - 1;

  /* encoding to label the data page */
  private final EncodeType encodingForDataPage;

  /* encoding to label the dictionary page */
  protected final EncodeType encodingForDictionaryPage;

  /* maximum size in bytes allowed for the dictionary will fail over to plain encoding if reached */
  protected final int maxDictionaryByteSize;

  /* will become true if the dictionary becomes too big */
  protected boolean dictionaryTooBig;

  /* current size in bytes the dictionary will take once serialized */
  protected int dictionaryByteSize;

  /* size in bytes of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  protected int lastUsedDictionaryByteSize;

  /* size in items of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
  protected int lastUsedDictionarySize;

  /* dictionary encoded values */
  protected IntList encodedValues = new IntList();

  /**
   * @param maxDictionaryByteSize
   */
  protected DictionaryValuesWriter(int maxDictionaryByteSize, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.encodingForDataPage = encodingForDataPage;
    this.encodingForDictionaryPage = encodingForDictionaryPage;
  }

  protected DictionaryPage dictPage(ValuesWriter dictionaryEncoder) {
    return new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, encodingForDictionaryPage);
  }


  @Override
  public long getBufferedSize() {
    return encodedValues.size() * 4;
  }

  @Override
  public long getAllocatedSize() {
    // size used in memory
    return encodedValues.size() * 4 + dictionaryByteSize;
  }

  @Override
  public BytesInput getBytes() {
    int maxDicId = getDictionarySize() - 1;
    if (LOG.DEBUG) LOG.debug("max dic id " + maxDicId);
    int bitWidth = BytesUtils.getWidthFromMaxInt(maxDicId);
    // TODO: what is a good initialCapacity?
    RunLengthBitPackingHybridEncoder encoder = RunLengthBitPackingHybridEncoder.newIntEncoder(bitWidth, false, 64 * 1024);
    IntList.IntIterator iterator = encodedValues.iterator();
    try {
      while (iterator.hasNext()) {
        encoder.writeInt(iterator.next());
      }
      // encodes the bit width
      byte[] bytesHeader = new byte[] { (byte) bitWidth };
      BytesInput rleEncodedBytes = encoder.toBytes();
      if (LOG.DEBUG) LOG.debug("rle encoded bytes " + rleEncodedBytes.size());
      BytesInput bytes = concat(BytesInput.from(bytesHeader), rleEncodedBytes);
      // remember size of dictionary when we last wrote a page
      lastUsedDictionarySize = getDictionarySize();
      lastUsedDictionaryByteSize = dictionaryByteSize;
      return bytes;
    } catch (IOException e) {
      throw new ZebraEncodingException("could not encode the values", e);
    }
  }

  @Override
  public EncodeType getEncoding() {
    return encodingForDataPage;
  }

  @Override
  public void reset() {
    encodedValues = new IntList();
  }

  @Override
  public void resetDictionary() {
    lastUsedDictionaryByteSize = 0;
    lastUsedDictionarySize = 0;
    dictionaryTooBig = false;
    clearDictionaryContent();
  }

  /**
   * clear/free the underlying dictionary content
   */
  protected abstract void clearDictionaryContent();

  /**
   * @return size in items
   */
  protected abstract int getDictionarySize();

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s DictionaryValuesWriter{\n"
          + "%s\n"
          + "%s\n"
        + "%s}\n",
        prefix,
        prefix + " dict:" + dictionaryByteSize,
        prefix + " values:" + String.valueOf(encodedValues.size() * 4),
        prefix
        );
  }

  /**
   *
   */
  public static class PlainBinaryDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    protected Object2IntMap<Binary> binaryDictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();

    /**
     * @param maxDictionaryByteSize
     */
    public PlainBinaryDictionaryValuesWriter(int maxDictionaryByteSize, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
      binaryDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeBytes(Binary v) {
      int id = binaryDictionaryContent.getInt(v);
      if (id == -1) {
        id = binaryDictionaryContent.size();
        binaryDictionaryContent.put(copy(v), id);
        // length as int (4 bytes) + actual bytes
        dictionaryByteSize += 4 + v.length();
      }
      encodedValues.add(id);
    }

    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          Binary entry = binaryIterator.next();
          dictionaryEncoder.writeBytes(entry);
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return binaryDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      binaryDictionaryContent.clear();
    }

    protected static Binary copy(Binary binary) {
      return Binary.fromByteArray(
          Arrays.copyOf(binary.getBytes(), binary.length()));
    }
  }

  /**
   *
   */
  public static class PlainFixedLenArrayDictionaryValuesWriter extends PlainBinaryDictionaryValuesWriter {

    private final int length;

    /**
     * @param maxDictionaryByteSize
     * @param length
     */
    public PlainFixedLenArrayDictionaryValuesWriter(int maxDictionaryByteSize, int length, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
      this.length = length;
    }

    @Override
    public void writeBytes(Binary value) {
      int id = binaryDictionaryContent.getInt(value);
      if (id == -1) {
        id = binaryDictionaryContent.size();
        binaryDictionaryContent.put(copy(value), id);
        dictionaryByteSize += length;
      }
      encodedValues.add(id);
    }

    @Override
    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        FixedLenByteArrayPlainValuesWriter dictionaryEncoder = new FixedLenByteArrayPlainValuesWriter(length, lastUsedDictionaryByteSize);
        Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          Binary entry = binaryIterator.next();
          dictionaryEncoder.writeBytes(entry);
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }
  }

  /**
   *
   */
  public static class PlainLongDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Long2IntMap longDictionaryContent = new Long2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     */
    public PlainLongDictionaryValuesWriter(int maxDictionaryByteSize, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
      longDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeLong(long v) {
      int id = longDictionaryContent.get(v);
      if (id == -1) {
        id = longDictionaryContent.size();
        longDictionaryContent.put(v, id);
        dictionaryByteSize += 8;
      }
      encodedValues.add(id);
    }

    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        LongIterator longIterator = longDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeLong(longIterator.nextLong());
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return longDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      longDictionaryContent.clear();
    }

  }

  /**
   *
   */
  public static class PlainDoubleDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Double2IntMap doubleDictionaryContent = new Double2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     *
     */
    public PlainDoubleDictionaryValuesWriter(int maxDictionaryByteSize, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
      doubleDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeDouble(double v) {
      int id = doubleDictionaryContent.get(v);
      if (id == -1) {
        id = doubleDictionaryContent.size();
        doubleDictionaryContent.put(v, id);
        dictionaryByteSize += 8;
      }
      encodedValues.add(id);
    }

    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        DoubleIterator doubleIterator = doubleDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeDouble(doubleIterator.nextDouble());
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return doubleDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      doubleDictionaryContent.clear();
    }

  }

  /**
   *
   */
  public static class PlainIntegerDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Int2IntMap intDictionaryContent = new Int2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     */
    public PlainIntegerDictionaryValuesWriter(int maxDictionaryByteSize, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
      intDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeInteger(int v) {
      int id = intDictionaryContent.get(v);
      if (id == -1) {
        id = intDictionaryContent.size();
        intDictionaryContent.put(v, id);
        dictionaryByteSize += 4;
      }
      encodedValues.add(id);
    }

    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        it.unimi.dsi.fastutil.ints.IntIterator intIterator = intDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeInteger(intIterator.nextInt());
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return intDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      intDictionaryContent.clear();
    }

  }

  /**
   *
   */
  public static class PlainFloatDictionaryValuesWriter extends DictionaryValuesWriter {

    /* type specific dictionary content */
    private Float2IntMap floatDictionaryContent = new Float2IntLinkedOpenHashMap();

    /**
     * @param maxDictionaryByteSize
     */
    public PlainFloatDictionaryValuesWriter(int maxDictionaryByteSize, EncodeType encodingForDataPage, EncodeType encodingForDictionaryPage) {
      super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
      floatDictionaryContent.defaultReturnValue(-1);
    }

    @Override
    public void writeFloat(float v) {
      int id = floatDictionaryContent.get(v);
      if (id == -1) {
        id = floatDictionaryContent.size();
        floatDictionaryContent.put(v, id);
        dictionaryByteSize += 4;
      }
      encodedValues.add(id);
    }

    public DictionaryPage createDictionaryPage() {
      if (lastUsedDictionarySize > 0) {
        // return a dictionary only if we actually used it
        PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize);
        FloatIterator floatIterator = floatDictionaryContent.keySet().iterator();
        // write only the part of the dict that we used
        for (int i = 0; i < lastUsedDictionarySize; i++) {
          dictionaryEncoder.writeFloat(floatIterator.nextFloat());
        }
        return dictPage(dictionaryEncoder);
      }
      return null;
    }

    @Override
    public int getDictionarySize() {
      return floatDictionaryContent.size();
    }

    @Override
    protected void clearDictionaryContent() {
      floatDictionaryContent.clear();
    }

  }

}
