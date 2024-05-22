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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

public class InMemoryFileIO implements FileIO, SupportsPrefixOperations {

  private static final Map<String, Pair<FileInfo, byte[]>> IN_MEMORY_FILES =
      Maps.newConcurrentMap();
  private boolean closed = false;

  public void addFile(String location, byte[] contents) {
    Preconditions.checkState(!closed, "Cannot call addFile after calling close()");
    FileInfo fileInfo = new FileInfo(location, contents.length, System.currentTimeMillis());
    IN_MEMORY_FILES.put(location, Pair.of(fileInfo, contents));
  }

  public boolean fileExists(String location) {
    return IN_MEMORY_FILES.containsKey(location);
  }

  @Override
  public InputFile newInputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newInputFile after calling close()");
    Pair<FileInfo, byte[]> file = IN_MEMORY_FILES.get(location);
    if (null == file) {
      throw new NotFoundException("No in-memory file found for location: %s", location);
    }
    return new InMemoryInputFile(location, file.second());
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

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return IN_MEMORY_FILES.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .map(entry -> entry.getValue().first())
        .collect(Collectors.toList());
  }

  @Override
  public void deletePrefix(String prefix) {
    List<String> matchedFiles =
        IN_MEMORY_FILES.keySet().stream()
            .filter(key -> key.startsWith(prefix))
            .collect(Collectors.toList());
    for (String toDelete : matchedFiles) {
      IN_MEMORY_FILES.remove(toDelete);
    }
  }
}
