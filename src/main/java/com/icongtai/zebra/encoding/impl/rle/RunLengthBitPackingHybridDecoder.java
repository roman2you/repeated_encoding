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


import com.icongtai.zebra.encoding.common.Log;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 */
abstract public class RunLengthBitPackingHybridDecoder {
    private static final Log LOG = Log.getLog(RunLengthBitPackingHybridDecoder.class);

    protected static enum MODE {RLE, PACKED}

    protected int bitWidth;

    protected InputStream in;

    protected MODE mode;
    protected boolean signed;

    public int readInt() throws IOException {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public long readLong() throws IOException {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public abstract int getCurrentCount();

    public static RunLengthBitPackingHybridDecoder newIntDecoder(InputStream in) {
        return new RunLengthBitPackingHybridForIntDecoder(in);
    }

    public static RunLengthBitPackingHybridDecoder newLongDecoder(InputStream in) {
        return new RunLengthBitPackingHybridForLongDecoder(in);
    }

}
