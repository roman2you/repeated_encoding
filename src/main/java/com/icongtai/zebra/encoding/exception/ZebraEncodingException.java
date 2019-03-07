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
package com.icongtai.zebra.encoding.exception;


/**
 * thrown when a decoding problem occured
 *
 * @author Julien Le Dem
 *
 */
public class ZebraEncodingException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ZebraEncodingException() {
  }

  public ZebraEncodingException(String message, Throwable cause) {
    super(message, cause);
  }

  public ZebraEncodingException(String message) {
    super(message);
  }

  public ZebraEncodingException(Throwable cause) {
    super(cause);
  }

}
