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
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableMap;

public class InMemoryFileIO implements FileIO, SupportsPrefixOperations {

  private static class InMemoryFile extends FileInfo {
    private final byte[] content;

    private InMemoryFile(String location, long size, long createdAtMillis, byte[] content) {
      super(location, size, createdAtMillis);
      Preconditions.checkArgument(content != null, "Invalid content: null");
      this.content = content;
    }
  }

  private static final Map<String, InMemoryFile> IN_MEMORY_FILES = Maps.newConcurrentMap();
  private boolean closed = false;
  private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  public void addFile(String location, byte[] contents) {
    Preconditions.checkState(!closed, "Cannot call addFile after calling close()");
    IN_MEMORY_FILES.put(
        location,
        new InMemoryFile(location, contents.length, System.currentTimeMillis(), contents));
  }

  public byte[] contents(String location) {
    Preconditions.checkState(!closed, "Cannot call contents after calling close()");
    InMemoryFile info = IN_MEMORY_FILES.get(location);
    if (info != null) {
      return info.content;
    }

    throw new NotFoundException("No in-memory file found for location: %s", location);
  }

  public boolean fileExists(String location) {
    return IN_MEMORY_FILES.containsKey(location);
  }

  @Override
  public InputFile newInputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newInputFile after calling close()");
    return new InMemoryInputFile(location, this);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newOutputFile after calling close()");
    return new InMemoryOutputFile(location, this);
  }

  @Override
  public void deleteFile(String location) {
    Preconditions.checkState(!closed, "Cannot call deleteFile after calling close()");
    if (null == IN_MEMORY_FILES.remove(location)) {
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

  public void clear() {
    IN_MEMORY_FILES.clear();
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return Iterables.transform(
        Iterables.filter(
            IN_MEMORY_FILES.values(), (InMemoryFile file) -> file.location().startsWith(prefix)),
        FileInfo.class::cast);
  }

  @Override
  public void deletePrefix(String prefix) {
    listPrefix(prefix).forEach(info -> deleteFile(info.location()));
  }
}
