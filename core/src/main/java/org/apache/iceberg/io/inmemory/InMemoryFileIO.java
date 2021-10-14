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

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * FileIO implementation backed by in-memory data-structures.
 * This class doesn't touch external resources and
 * can be utilized to write unit tests without side effects.
 * Locations can any string supported by the {@link InMemoryFileStore}.
 */
public final class InMemoryFileIO implements FileIO {

  private final InMemoryFileStore store;

  public InMemoryFileIO() {
    this(InMemoryFileStoreFactory.create());
  }

  public InMemoryFileIO(InMemoryFileStore store) {
    this.store = store;
  }

  public InMemoryFileStore getStore() {
    return store;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new InMemoryInputFile(store, path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new InMemoryOutputFile(store, path);
  }

  @Override
  public void deleteFile(String path) {
    if (!store.remove(path)) {
      throw new IllegalArgumentException("Location: " + path + " does not exist!");
    }
  }
}
