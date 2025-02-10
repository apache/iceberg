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

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;

/**
 * Base class for the appender based writer implementations ({@link DataWriterBuilderBase} and
 * {@link EqualityDeleteWriterBuilderBase}.
 *
 * @param <T> the type of the builder for chaining
 */
class FileWriterBuilderBase<T extends FileWriterBuilderBase<T>> implements FileWriterBuilder<T> {
  private final AppenderBuilder<?> appenderBuilder;
  private final FileFormat format;
  private PartitionSpec spec;
  private StructLike partition;
  private EncryptionKeyMetadata keyMetadata = null;
  private SortOrder sortOrder;

  FileWriterBuilderBase(AppenderBuilder<?> appenderBuilder, FileFormat format) {
    this.appenderBuilder = appenderBuilder;
    this.format = format;
  }

  @Override
  public T forTable(Table table) {
    schema(table.schema());
    withSpec(table.spec());
    setAll(table.properties());
    metricsConfig(org.apache.iceberg.MetricsConfig.forTable(table));
    return (T) this;
  }

  @Override
  public T schema(Schema newSchema) {
    appenderBuilder.schema(newSchema);
    return (T) this;
  }

  @Override
  public T set(String property, String value) {
    appenderBuilder.set(property, value);
    return (T) this;
  }

  @Override
  public T setAll(Map<String, String> properties) {
    appenderBuilder.setAll(properties);
    return (T) this;
  }

  @Override
  public T meta(String property, String value) {
    appenderBuilder.meta(property, value);
    return (T) this;
  }

  @Override
  public T meta(Map<String, String> properties) {
    appenderBuilder.meta(properties);
    return (T) this;
  }

  @Override
  public T overwrite() {
    return overwrite(true);
  }

  @Override
  public T overwrite(boolean enabled) {
    appenderBuilder.overwrite(enabled);
    return (T) this;
  }

  @Override
  public T metricsConfig(MetricsConfig newMetricsConfig) {
    appenderBuilder.metricsConfig(newMetricsConfig);
    return (T) this;
  }

  @Override
  public T withSpec(PartitionSpec newSpec) {
    this.spec = newSpec;
    return (T) this;
  }

  @Override
  public T withPartition(StructLike newPartition) {
    this.partition = newPartition;
    return (T) this;
  }

  @Override
  public T withKeyMetadata(EncryptionKeyMetadata metadata) {
    this.keyMetadata = metadata;
    return (T) this;
  }

  @Override
  public T withSortOrder(SortOrder newSortOrder) {
    this.sortOrder = newSortOrder;
    return (T) this;
  }

  @Override
  public String location() {
    return appenderBuilder().location();
  }

  @Override
  public Schema schema() {
    return appenderBuilder().schema();
  }

  protected AppenderBuilder<?> appenderBuilder() {
    return appenderBuilder;
  }

  protected FileFormat format() {
    return format;
  }

  protected PartitionSpec spec() {
    return spec;
  }

  protected StructLike partition() {
    return partition;
  }

  protected EncryptionKeyMetadata keyMetadata() {
    return keyMetadata;
  }

  protected SortOrder sortOrder() {
    return sortOrder;
  }
}
