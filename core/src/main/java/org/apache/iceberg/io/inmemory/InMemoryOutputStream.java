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

package org.apache.iceberg.io.inmemory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.iceberg.io.PositionOutputStream;

final class InMemoryOutputStream extends PositionOutputStream {

  private final Consumer<byte[]> putOnCloseAction;
  private final ByteArrayOutputStream outputStream;

  InMemoryOutputStream(Consumer<byte[]> putOnCloseAction) {
    this.putOnCloseAction = putOnCloseAction;
    this.outputStream = new ByteArrayOutputStream();
  }

  @Override
  public long getPos() throws IOException {
    return outputStream.size();
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void write(int i) throws IOException {
    outputStream.write(i);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    outputStream.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
    putOnCloseAction.accept(outputStream.toByteArray());
  }
}
