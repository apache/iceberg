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
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * A decorator that collapses multiple object-store requests into one by fetching the entire file
 * eagerly on the first call.
 */
public class EagerInputFile implements InputFile {

  private final InputFile delegate;
  private final long length;

  private EagerInputFile(InputFile delegate, long length) {
    Preconditions.checkNotNull(delegate, "delegate is null");
    Preconditions.checkArgument(length >= 0, "length is negative: %s", length);
    Preconditions.checkArgument(
        length <= Integer.MAX_VALUE,
        "Cannot eagerly load file because length exceeds eager loading capacity, consider reducing eager fetch threshold below %s bytes: %s",
        Integer.MAX_VALUE,
        length);
    this.delegate = delegate;
    this.length = length;
  }

  /**
   * Returns an {@link EagerInputFile}, preserving Hadoop config if {@code delegate} is {@link
   * HadoopConfigurable}.
   */
  public static InputFile of(InputFile delegate, long length) {
    if (delegate instanceof HadoopConfigurable) {
      return new EagerInputFileConfigurable(
          delegate, length, ((HadoopConfigurable) delegate).getConf());
    }
    return new EagerInputFile(delegate, length);
  }

  @Override
  public long getLength() {
    return length;
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
    byte[] bytes = new byte[(int) length];
    try (SeekableInputStream src = delegate.newStream()) {
      IOUtil.readFully(src, bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to fetch file: %s", delegate.location());
    }
    return new EagerInputStream(bytes);
  }

  /** An {@link EagerInputFile} that carries the delegate's Hadoop configuration. */
  private static class EagerInputFileConfigurable extends EagerInputFile
      implements HadoopConfigurable {

    private final Configuration conf;

    EagerInputFileConfigurable(InputFile delegate, long length, Configuration conf) {
      super(delegate, length);
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void serializeConfWith(
        Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
      // no-op: EagerInputFile is not serialized
    }
  }
}
