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
package org.apache.iceberg.parquet;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

class ParquetWriteSupport<T> extends WriteSupport<T> {
  private final MessageType type;
  private final Map<String, String> keyValueMetadata;
  private final WriteSupport<T> wrapped;

  ParquetWriteSupport(
      MessageType type, Map<String, String> keyValueMetadata, WriteSupport<T> writeSupport) {
    this.type = type;
    this.keyValueMetadata = keyValueMetadata;
    this.wrapped = writeSupport;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    WriteContext wrappedContext = wrapped.init(configuration);
    Map<String, String> metadata =
        ImmutableMap.<String, String>builder()
            .putAll(keyValueMetadata)
            .putAll(wrappedContext.getExtraMetaData())
            .buildOrThrow();
    return new WriteContext(type, metadata);
  }

  @Override
  public String getName() {
    return "Iceberg/" + wrapped.getName();
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    wrapped.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(T t) {
    wrapped.write(t);
  }

  @Override
  public FinalizedWriteContext finalizeWrite() {
    return wrapped.finalizeWrite();
  }
}
