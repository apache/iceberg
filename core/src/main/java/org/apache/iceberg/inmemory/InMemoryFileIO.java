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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;

public class InMemoryFileIO implements FileIO {

  /**
   * When set to {@code true}, reads and deletes for unknown locations fall back to local disk. This
   * is useful in test environments that mix in-memory writes (through this {@link FileIO}) with
   * files that were written to local disk through another path (for example, Spark's native {@code
   * DataFrameWriter} writing to a JUnit {@code @TempDir}). New writes always go to the in-memory
   * store regardless of this setting.
   */
  public static final String DISK_FALLBACK = "in-memory-file-io.disk-fallback";

  private static final Map<String, byte[]> IN_MEMORY_FILES = Maps.newConcurrentMap();
  private boolean closed = false;
  private boolean diskFallback = false;
  private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    this.diskFallback = PropertyUtil.propertyAsBoolean(props, DISK_FALLBACK, false);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  public void addFile(String location, byte[] contents) {
    Preconditions.checkState(!closed, "Cannot call addFile after calling close()");
    IN_MEMORY_FILES.put(location, contents);
  }

  public boolean fileExists(String location) {
    return IN_MEMORY_FILES.containsKey(location);
  }

  /**
   * Renames the in-memory entry at {@code from} to {@code to}, returning {@code true} on success
   * and {@code false} if no in-memory content is registered at {@code from}. The map is shared
   * across all {@link InMemoryFileIO} instances in the JVM, so callers do not need a specific
   * instance reference. This is intended for tests that need to swap or hide an Iceberg-managed
   * file written through this {@link FileIO}.
   */
  public static boolean tryRename(String from, String to) {
    byte[] contents = IN_MEMORY_FILES.remove(from);
    if (contents == null) {
      return false;
    }
    IN_MEMORY_FILES.put(to, contents);
    return true;
  }

  @Override
  public InputFile newInputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newInputFile after calling close()");
    byte[] contents = IN_MEMORY_FILES.get(location);
    if (contents != null) {
      return new InMemoryInputFile(location, contents);
    }
    if (diskFallback) {
      InputFile diskInput = localInputIfExists(location);
      if (diskInput != null) {
        return diskInput;
      }
    }
    throw new NotFoundException("No in-memory file found for location: %s", location);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    Preconditions.checkState(!closed, "Cannot call newOutputFile after calling close()");
    return new InMemoryOutputFile(location, this);
  }

  @Override
  public void deleteFile(String location) {
    Preconditions.checkState(!closed, "Cannot call deleteFile after calling close()");
    if (IN_MEMORY_FILES.remove(location) != null) {
      return;
    }
    if (diskFallback && deleteLocalIfExists(location)) {
      return;
    }
    throw new NotFoundException("No in-memory file found for location: %s", location);
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    closed = true;
  }

  private static InputFile localInputIfExists(String location) {
    File file = localFile(location);
    if (file == null || !file.exists()) {
      return null;
    }
    return Files.localInput(file);
  }

  private static boolean deleteLocalIfExists(String location) {
    File file = localFile(location);
    if (file == null || !file.exists()) {
      return false;
    }
    if (!file.delete()) {
      throw new UncheckedIOException(new IOException("Failed to delete file: " + location));
    }
    return true;
  }

  private static File localFile(String location) {
    if (location == null) {
      return null;
    }
    String path = location.startsWith("file:") ? location.substring("file:".length()) : location;
    return new File(path);
  }
}
