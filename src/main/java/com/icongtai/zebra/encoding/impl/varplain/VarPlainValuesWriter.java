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

import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import com.icongtai.zebra.encoding.bytes.CapacityByteArrayOutputStream;
import com.icongtai.zebra.encoding.bytes.LittleEndianDataOutputStream;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.format.EncodeType;

import java.io.IOException;
import java.nio.charset.Charset;


/**
 * Plain encoding except for booleans
 *
 */
public class VarPlainValuesWriter extends ValuesWriter {
  private static final Log LOG = Log.getLog(VarPlainValuesWriter.class);

  public static final Charset CHARSET = Charset.forName("UTF-8");

  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;
  private boolean signed;

  public VarPlainValuesWriter(int initialSize, boolean signed) {
    this.signed = signed;
    arrayOut = new CapacityByteArrayOutputStream(initialSize);
    out = new LittleEndianDataOutputStream(arrayOut);
  }

  @Override
  public final void writeInteger(int v) {
    try {
      if(!signed) {
        BytesUtils.writeUnsignedVarInt(v, out);
      } else {
        BytesUtils.writeZigZagVarInt(v, out);
      }
    } catch (IOException e) {
      throw new ZebraEncodingException("could not write int", e);
    }
  }

  @Override
  public final void writeLong(long v) {
    try {
      if(!signed) {
        BytesUtils.writeUnsignedVarLong(v, out);
      } else {
        BytesUtils.writeZigZagVarLong(v, out);
      }
    } catch (IOException e) {
      throw new ZebraEncodingException("could not write long", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ZebraEncodingException("could not write page", e);
    }
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

  @Override
  public long getAllocatedSize() {
    return arrayOut.getCapacity();
  }

  @Override
  public EncodeType getEncoding() {
    return EncodeType.var_plain;
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(prefix + " VAR_PLAIN");
  }

}
