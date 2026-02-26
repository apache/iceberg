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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.SerializableFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.util.KnownSizeEstimation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializableFileIOWithSize extends SerializableFileIO
    implements KnownSizeEstimation, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SerializableFileIOWithSize.class);
  private static final long SIZE_ESTIMATE = 32_768L;

  private final transient Object serializationMarker;

  protected SerializableFileIOWithSize(FileIO fileIO) {
    super(fileIO);
    this.serializationMarker = new Object();
  }

  @Override
  public long estimatedSize() {
    return SIZE_ESTIMATE;
  }

  public static FileIO copyOf(FileIO fileIO) {
    return new SerializableFileIOWithSize(SerializableFileIO.copyOf(fileIO));
  }

  @Override
  public void close() {
    if (serializationMarker == null) {
      LOG.info("Releasing resources");
      io().close();
    }
  }
}
