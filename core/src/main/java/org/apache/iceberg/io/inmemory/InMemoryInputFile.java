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

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

final class InMemoryInputFile implements InputFile {

  private final InMemoryFileStore store;
  private final String location;

  InMemoryInputFile(InMemoryFileStore store, String location) {
    this.store = store;
    this.location = location;
  }

  @Override
  public long getLength() {
    return getDataOrThrow().remaining();
  }

  @Override
  public SeekableInputStream newStream() {
    return new InMemoryInputStream(getDataOrThrow().duplicate());
  }

  private ByteBuffer getDataOrThrow() {
    return store.get(location).orElseThrow(
      () -> new UncheckedIOException(new FileNotFoundException("Cannot find file, does not exist: " + location))
    );
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    return store.exists(location);
  }
}
