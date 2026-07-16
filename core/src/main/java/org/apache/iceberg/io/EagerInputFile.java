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

import java.io.IOException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A decorator that collapses multiple object-store requests into one by fetching the entire file
 * eagerly on the first call.
 */
public class EagerInputFile implements InputFile {

  private final InputFile delegate;
  private final long fileSize;

  public EagerInputFile(InputFile delegate, long fileSize) {
    Preconditions.checkNotNull(delegate, "delegate is null");
    Preconditions.checkArgument(fileSize >= 0, "fileSize is negative: %s", fileSize);
    Preconditions.checkArgument(
        fileSize <= Integer.MAX_VALUE,
        "Cannot eagerly load file because fileSize exceeds eager loading capacity, consider reducing eager fetch threshold below %s bytes: %s",
        Integer.MAX_VALUE,
        fileSize);
    this.delegate = delegate;
    this.fileSize = fileSize;
  }

  @Override
  public long getLength() {
    return fileSize;
  }

  @Override
  public String location() {
    return delegate.location();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  @Override
  public SeekableInputStream newStream() {
    byte[] bytes = new byte[(int) fileSize];
    try (SeekableInputStream src = delegate.newStream()) {
      IOUtil.readFully(src, bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to fetch file: %s", delegate.location());
    }
    return new EagerInputStream(bytes);
  }
}
