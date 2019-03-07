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

import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.bytes.CapacityByteArrayOutputStream;
import com.icongtai.zebra.encoding.bytes.LittleEndianDataOutputStream;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.exception.ZebraEncodingException;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;

import java.io.IOException;


/**
 * Write lengths of byte-arrays using delta encoding, followed by concatenated byte-arrays
 * <pre>
 *   {@code
 *   delta-length-byte-array : length* byte-array*
 *   } 
 * </pre>
 *
 */
public class DeltaLengthByteArrayValuesWriter extends ValuesWriter {

  private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesWriter.class);

  private ValuesWriter lengthWriter;
  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;

  public DeltaLengthByteArrayValuesWriter(int initialSize) {
    arrayOut = new CapacityByteArrayOutputStream(initialSize);
    out = new LittleEndianDataOutputStream(arrayOut);
    lengthWriter = new DeltaIntBitPackingValuesWriter(
            DeltaIntBitPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
            DeltaIntBitPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
        initialSize);
  }

  @Override
  public void writeBytes(Binary v) {
    try {
      lengthWriter.writeInteger(v.length());
      out.write(v.getBytes());
    } catch (IOException e) {
      throw new ZebraEncodingException("could not write bytes", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return lengthWriter.getBufferedSize() + arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ZebraEncodingException("could not write page", e);
    }
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(arrayOut));
  }

  @Override
  public EncodeType getEncoding() {
    return EncodeType.delta_length_byte_array;
  }

  @Override
  public void reset() {
    lengthWriter.reset();
    arrayOut.reset();
  }

  @Override
  public long getAllocatedSize() {
    return lengthWriter.getAllocatedSize() + arrayOut.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(lengthWriter.memUsageString(prefix) + " DELTA_LENGTH_BYTE_ARRAY"); 
  }
}
