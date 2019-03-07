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
package com.icongtai.zebra.encoding.impl.varplain;


import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.bytes.LittleEndianDataInputStream;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;


/**
 * Plain encoding for float, double, int, long
 *
 */
abstract public class VarPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(VarPlainValuesReader.class);

  protected LittleEndianDataInputStream in;

  protected boolean signed = true;

  public VarPlainValuesReader(boolean signed) {
    this.signed = signed;
  }

  /**
   * {@inheritDoc}
   * @see ValuesReader(byte[], int)
   */
  @Override
  public void init(int valueCount, byte[] in, int offset) throws IOException {
    if (LOG.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = new LittleEndianDataInputStream(new ByteArrayInputStream(in, offset, in.length - offset));
  }

  public static class VarIntegerPlainValuesReader extends VarPlainValuesReader {

    public VarIntegerPlainValuesReader(boolean unsigned) {
      super(unsigned);
    }

    @Override
    public void skip() {
      throw new ZebraDecodingException("could not skip varInt");
    }

    @Override
    public int readInteger() {
      try {
        if(!signed) {
          return BytesUtils.readUnsignedVarInt(in);
        } else {
          return BytesUtils.readZigZagVarInt(in);
        }
      } catch (IOException e) {
        throw new ZebraDecodingException("could not read int", e);
      }
    }
  }

  public static class VarLongPlainValuesReader extends VarPlainValuesReader {

    public VarLongPlainValuesReader(boolean unsigned) {
      super(unsigned);
    }

    @Override
    public void skip() {
      throw new ZebraDecodingException("could not skip varLong");
    }

    @Override
    public long readLong() {
      try {
        if(!signed) {
          return BytesUtils.readUnsignedVarLong(in);
        } else {
          return BytesUtils.readZigZagVarLong(in);
        }
      } catch (IOException e) {
        throw new ZebraDecodingException("could not read int", e);
      }
    }
  }
}