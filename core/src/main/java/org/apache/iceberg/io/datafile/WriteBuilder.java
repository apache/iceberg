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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;

interface WriteBuilder<D> {

  /** Sets the {@link Table} specific parameters like schema, spec, etc. all at once. */
  WriteBuilder<D> forTable(Table table);

  /** Sets configuration key/value pairs for the writer. */
  WriteBuilder<D> set(String property, String value);

  WriteBuilder<D> setAll(Map<String, String> properties);

  /** Sets the file metadata kep/value pairs for the writer which should be written to the file. */
  WriteBuilder<D> meta(String property, String value);

  default WriteBuilder<D> meta(Map<String, String> properties) {
    properties.forEach(this::meta);
    return this;
  }

  /** Enables overwriting previously created files. */
  WriteBuilder<D> overwrite();

  WriteBuilder<D> overwrite(boolean enabled);

  /**
   * Sets the configuration for collecting file metrics. Writers should provide metrics for metadata
   * based on this configuration.
   */
  WriteBuilder<D> metricsConfig(MetricsConfig newMetricsConfig);

  WriteBuilder<D> withSpec(PartitionSpec newSpec);

  WriteBuilder<D> withPartition(StructLike newPartition);

  WriteBuilder<D> withKeyMetadata(EncryptionKeyMetadata metadata);

  WriteBuilder<D> withSortOrder(SortOrder newSortOrder);

  /**
   * The record schema the equality delete writes. Could be different from the actual table schema.
   */
  WriteBuilder<D> rowSchema(Schema schema);

  default WriteBuilder<D> withFileEncryptionKey(ByteBuffer fileEncryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  default WriteBuilder<D> withAADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  WriteBuilder<D> equalityFieldIds(List<Integer> fieldIds);

  /** Sets the equality field ids which are used in the delete file. */
  WriteBuilder<D> equalityFieldIds(int... fieldIds);

  EqualityDeleteWriter<D> appenderBuilder() throws IOException;

  EqualityDeleteWriter<D> writerBuilder() throws IOException;

  EqualityDeleteWriter<D> equalityWriterBuilder() throws IOException;

  PositionDeleteWriter<D> positionWriterBuilder() throws IOException;
}
