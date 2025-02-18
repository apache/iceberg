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
package org.apache.iceberg.io.datafile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;

/** Builder API for creating {@link DataWriter}s. */
public interface DataWriterBuilder {
  /** Sets the {@link Table} specific parameters like schema, spec, etc. all at once. */
  DataWriterBuilder forTable(Table table);

  /** Sets the write schema for the writer. */
  DataWriterBuilder schema(Schema schema);

  /** Sets configuration key/value pairs for the writer. */
  DataWriterBuilder set(String property, String value);

  DataWriterBuilder setAll(Map<String, String> properties);

  /** Sets the file metadata kep/value pairs for the writer which should be written to the file. */
  DataWriterBuilder meta(String property, String value);

  default DataWriterBuilder meta(Map<String, String> properties) {
    properties.forEach(this::meta);
    return this;
  }

  /** Enables overwriting previously created files. */
  DataWriterBuilder overwrite();

  DataWriterBuilder overwrite(boolean enabled);

  /**
   * Sets the configuration for collecting file metrics. Writers should provide metrics for metadata
   * based on this configuration.
   */
  DataWriterBuilder metricsConfig(MetricsConfig newMetricsConfig);

  DataWriterBuilder withSpec(PartitionSpec newSpec);

  DataWriterBuilder withPartition(StructLike newPartition);

  DataWriterBuilder withKeyMetadata(EncryptionKeyMetadata metadata);

  DataWriterBuilder withSortOrder(SortOrder newSortOrder);

  default DataWriterBuilder withFileEncryptionKey(ByteBuffer fileEncryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  default DataWriterBuilder withAADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  <D> DataWriter<D> build() throws IOException;
}
