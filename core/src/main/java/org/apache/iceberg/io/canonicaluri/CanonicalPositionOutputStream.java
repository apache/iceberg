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
package org.apache.iceberg.io.canonicaluri;

import java.io.IOException;
import org.apache.iceberg.io.PositionOutputStream;

/** Retains a delegate stream without exposing it for downstream adapter unwrapping. */
final class CanonicalPositionOutputStream extends PositionOutputStream {
  private final PositionOutputStream delegate;

  CanonicalPositionOutputStream(PositionOutputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public long storedLength() throws IOException {
    return delegate.storedLength();
  }

  @Override
  public void write(int value) throws IOException {
    delegate.write(value);
  }

  @Override
  public void write(byte[] buffer) throws IOException {
    delegate.write(buffer);
  }

  @Override
  public void write(byte[] buffer, int off, int len) throws IOException {
    delegate.write(buffer, off, len);
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
