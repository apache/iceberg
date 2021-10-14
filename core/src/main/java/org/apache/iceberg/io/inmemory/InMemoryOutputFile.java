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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

final class InMemoryOutputFile implements OutputFile {

  private final InMemoryFileStore store;
  private final String location;

  InMemoryOutputFile(InMemoryFileStore store, String location) {
    this.store = store;
    this.location = location;
  }

  @Override
  public PositionOutputStream create() {
    if (store.putIfAbsent(location, new byte[0]) != null) {
      throw new UncheckedIOException(new IOException("Location: " + location + " already exists!"));
    }
    return new InMemoryOutputStream(data -> store.put(location, data));
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    store.put(location, new byte[0]);
    return new InMemoryOutputStream(data -> store.put(location, data));
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    return new InMemoryInputFile(store, location);
  }
}
