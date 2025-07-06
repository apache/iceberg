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
package org.apache.iceberg.aws.s3;

import java.io.IOException;
import org.apache.iceberg.io.SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;

/** A wrapper to convert {@link S3SeekableInputStream} to Iceberg {@link SeekableInputStream} */
class AnalyticsAcceleratorInputStreamWrapper extends SeekableInputStream {

  private final S3SeekableInputStream delegate;

  AnalyticsAcceleratorInputStreamWrapper(S3SeekableInputStream stream) {
    this.delegate = stream;
  }

  @Override
  public int read() throws IOException {
    return this.delegate.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return this.delegate.read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return this.delegate.read(b, off, len);
  }

  @Override
  public void seek(long l) throws IOException {
    this.delegate.seek(l);
  }

  @Override
  public long getPos() {
    return this.delegate.getPos();
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }
}
