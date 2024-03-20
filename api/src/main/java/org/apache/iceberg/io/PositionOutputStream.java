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
package org.apache.iceberg.io;

import java.io.IOException;
import java.io.OutputStream;

public abstract class PositionOutputStream extends OutputStream {
  /**
   * Return the current position in the OutputStream.
   *
   * @return current position in bytes from the start of the stream
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract long getPos() throws IOException;

  /**
   * Return the current stored length of the output. Can differ from the current position for
   * encrypting streams, and for other non-length-preserving streams.
   *
   * @return current stored length in bytes
   * @throws IOException If the underlying stream throws IOException
   */
  public long storedLength() throws IOException {
    return getPos();
  }
}
