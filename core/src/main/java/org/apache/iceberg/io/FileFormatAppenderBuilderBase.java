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

import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Base implementation for the {@link FileFormatReadBuilder} which handles the common attributes for
 * the builders.
 *
 * @param <T> the type of the builder which is needed so method chaining is available for the
 *     builder
 */
public abstract class FileFormatAppenderBuilderBase<T extends FileFormatAppenderBuilderBase<T>>
    implements FileFormatAppenderBuilder<T> {
  private final OutputFile file;
  private final String location;
  private final Map<String, String> config = Maps.newHashMap();
  private final Map<String, String> metadata = Maps.newLinkedHashMap();
  private Schema schema;
  private boolean overwrite;
  private MetricsConfig metricsConfig = MetricsConfig.getDefault();
  private String name = "table";

  public FileFormatAppenderBuilderBase(OutputFile file) {
    this.file = file;
    this.location = file.location();
  }

  @Override
  public T forTable(Table table) {
    schema(table.schema());
    setAll(table.properties());
    metricsConfig(MetricsConfig.forTable(table));
    return (T) this;
  }

  @Override
  public T schema(Schema newSchema) {
    this.schema = newSchema;
    return (T) this;
  }

  @Override
  public T set(String property, String value) {
    config.put(property, value);
    return (T) this;
  }

  @Override
  public T setAll(Map<String, String> properties) {
    config.putAll(properties);
    return (T) this;
  }

  @Override
  public T meta(String property, String value) {
    metadata.put(property, value);
    return (T) this;
  }

  @Override
  public T meta(Map<String, String> properties) {
    metadata.putAll(properties);
    return (T) this;
  }

  @Override
  public T metricsConfig(MetricsConfig newMetricsConfig) {
    this.metricsConfig = newMetricsConfig;
    return (T) this;
  }

  @Override
  public T overwrite() {
    return overwrite(true);
  }

  @Override
  public T overwrite(boolean enabled) {
    this.overwrite = enabled;
    return (T) this;
  }

  @Override
  public T named(String newName) {
    this.name = newName;
    return (T) this;
  }

  @Override
  public String location() {
    return location;
  }

  protected OutputFile file() {
    return file;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  protected Map<String, String> config() {
    return config;
  }

  protected Map<String, String> metadata() {
    return metadata;
  }

  protected boolean isOverwrite() {
    return overwrite;
  }

  protected MetricsConfig metricsConfig() {
    return metricsConfig;
  }

  protected String name() {
    return name;
  }
}
