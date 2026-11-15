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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class CanonicalUriFileIO implements DelegateFileIO {

  private final FileIO delegate;
  private final String canonicalPrefix;
  private final String physicalPrefix;

  public CanonicalUriFileIO(FileIO delegate, String canonicalPrefix, String physicalPrefix) {
    if (canonicalPrefix == null || canonicalPrefix.isEmpty()) {
      throw new IllegalArgumentException("Canonical URI prefix must not be null or empty");
    }

    if (physicalPrefix == null || physicalPrefix.isEmpty()) {
      throw new IllegalArgumentException("Physical URI prefix must not be null or empty");
    }

    if (!canonicalPrefix.endsWith("/") || !physicalPrefix.endsWith("/")) {
      throw new IllegalArgumentException("URI mapping prefixes must end with '/'");
    }

    this.delegate = delegate;
    this.canonicalPrefix = canonicalPrefix;
    this.physicalPrefix = physicalPrefix;
  }

  private String resolve(String location) {
    if (location.startsWith(canonicalPrefix)) {
      return physicalPrefix + location.substring(canonicalPrefix.length());
    }

    return location;
  }

  @Override
  public InputFile newInputFile(String location) {
    return wrapInput(delegate.newInputFile(resolve(location)), location);
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    return wrapInput(delegate.newInputFile(resolve(location), length), location);
  }

  private InputFile wrapInput(InputFile physical, String canonical) {
    return new InputFile() {
      @Override
      public long getLength() {
        return physical.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        return physical.newStream();
      }

      @Override
      public String location() {
        return canonical;
      }

      @Override
      public boolean exists() {
        return physical.exists();
      }
    };
  }

  @Override
  public OutputFile newOutputFile(String location) {
    OutputFile physical = delegate.newOutputFile(resolve(location));

    return new OutputFile() {
      @Override
      public PositionOutputStream create() {
        return physical.create();
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return physical.createOrOverwrite();
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public InputFile toInputFile() {
        return wrapInput(physical.toInputFile(), location);
      }
    };
  }

  @Override
  public void deleteFile(String location) {
    delegate.deleteFile(resolve(location));
  }

  private String canonicalize(String location) {
    if (location.startsWith(physicalPrefix)) {
      return canonicalPrefix + location.substring(physicalPrefix.length());
    }

    return location;
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    List<String> physicalPaths = Lists.newArrayList();

    for (String path : pathsToDelete) {
      physicalPaths.add(resolve(path));
    }

    if (delegate instanceof SupportsBulkOperations) {
      ((SupportsBulkOperations) delegate).deleteFiles(physicalPaths);
    } else {
      for (String path : physicalPaths) {
        delegate.deleteFile(path);
      }
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    if (!(delegate instanceof SupportsPrefixOperations)) {
      throw new UnsupportedOperationException("Delegate does not support prefix listing");
    }

    List<FileInfo> results = Lists.newArrayList();

    for (FileInfo file : ((SupportsPrefixOperations) delegate).listPrefix(resolve(prefix))) {
      results.add(new FileInfo(canonicalize(file.location()), file.size(), file.createdAtMillis()));
    }

    return results;
  }

  @Override
  public void deletePrefix(String prefix) {
    if (!(delegate instanceof SupportsPrefixOperations)) {
      throw new UnsupportedOperationException("Delegate does not support prefix deletion");
    }

    ((SupportsPrefixOperations) delegate).deletePrefix(resolve(prefix));
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void close() {
    delegate.close();
  }
}
