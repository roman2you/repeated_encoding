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


import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.common.Log;
import com.icongtai.zebra.encoding.impl.bitpacking.ByteBitPackingValuesReader;

import java.io.IOException;


/**
 * encodes boolean for the plain encoding: one bit at a time (0 = false)
 *
 */
public class BooleanPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(BooleanPlainValuesReader.class);

  private ByteBitPackingValuesReader in = new ByteBitPackingValuesReader(1, Packer.LITTLE_ENDIAN);

  /**
   *
   * {@inheritDoc}
   * @see ValuesReader#readBoolean()
   */
  @Override
  public boolean readBoolean() {
    return in.readInteger() == 0 ? false : true;
  }

  /**
   * {@inheritDoc}
   * @see ValuesReader#skip()
   */
  @Override
  public void skip() {
    in.readInteger();
  }

  /**
   * {@inheritDoc}
   * @see ValuesReader (byte[], int)
   */
  @Override
  public void init(int valueCount, byte[] in, int offset) throws IOException {
    if (LOG.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in.init(valueCount, in, offset);
  }
  
  @Override
  public int getNextOffset() {
    return this.in.getNextOffset();
  }

}