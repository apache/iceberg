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
package org.apache.iceberg.inmemory;

import java.util.Map;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class InMemoryFileIO implements FileIO {

  private final Map<String, byte[]> inMemoryFiles = Maps.newConcurrentMap();
  private boolean closed = false;

  public void addFile(String location, byte[] contents) {
    Preconditions.checkState(!closed, "Cannot call addFile after calling close()");
    inMemoryFiles.put(location, contents);
  }

  public boolean fileExists(String location) {
    return inMemoryFiles.containsKey(location);
  }

  @Override
  public InputFile newInputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newInputFile after calling close()");
    byte[] contents = inMemoryFiles.get(location);
    if (null == contents) {
      throw new NotFoundException("No in-memory file found for location: %s", location);
    }
    return new InMemoryInputFile(location, contents);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newOutputFile after calling close()");
    return new InMemoryOutputFile(location, this);
  }

  @Override
  public void deleteFile(String location) {
    Preconditions.checkState(!closed, "Cannot call deleteFile after calling close()");
    if (null == inMemoryFiles.remove(location)) {
      throw new NotFoundException("No in-memory file found for location: %s", location);
    }
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    closed = true;
  }
}
