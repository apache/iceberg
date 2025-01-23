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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Base implementation for the {@link FileFormatReadBuilder} which handles the common attributes for
 * the builders.
 *
 * @param <T> the type of the builder which is needed so method chaining is available for the
 *     builder
 */
public abstract class FileFormatReadBuilderBase<T extends FileFormatReadBuilderBase<T>>
    implements FileFormatReadBuilder<T> {
  private final InputFile file;
  private final Map<String, String> properties = Maps.newHashMap();
  private Long start = null;
  private Long length = null;
  private Schema schema = null;
  private Expression filter = null;
  private boolean filterRecords = true;
  private boolean caseSensitive = true;
  private boolean reuseContainers = false;
  private int recordsPerBatch = 10000;
  private NameMapping nameMapping = null;
  private ByteBuffer fileEncryptionKey = null;
  private ByteBuffer fileAADPrefix = null;

  protected FileFormatReadBuilderBase(InputFile file) {
    Preconditions.checkNotNull(file, "Input file cannot be null");
    this.file = file;
  }

  @Override
  public T split(long newStart, long newLength) {
    this.start = newStart;
    this.length = newLength;
    return (T) this;
  }

  @Override
  public T project(Schema newSchema) {
    this.schema = newSchema;
    return (T) this;
  }

  @Override
  public T caseInsensitive() {
    return caseSensitive(false);
  }

  @Override
  public T caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return (T) this;
  }

  @Override
  public T filterRecords(boolean newFilterRecords) {
    this.filterRecords = newFilterRecords;
    return (T) this;
  }

  @Override
  public T filter(Expression newFilter) {
    this.filter = newFilter;
    return (T) this;
  }

  @Override
  public T set(String key, String value) {
    properties.put(key, value);
    return (T) this;
  }

  @Override
  public T reuseContainers() {
    this.reuseContainers = true;
    return (T) this;
  }

  @Override
  public T reuseContainers(boolean newReuseContainers) {
    this.reuseContainers = newReuseContainers;
    return (T) this;
  }

  @Override
  public T recordsPerBatch(int numRowsPerBatch) {
    this.recordsPerBatch = numRowsPerBatch;
    return (T) this;
  }

  @Override
  public T withNameMapping(NameMapping newNameMapping) {
    this.nameMapping = newNameMapping;
    return (T) this;
  }

  @Override
  public T withFileEncryptionKey(ByteBuffer encryptionKey) {
    this.fileEncryptionKey = encryptionKey;
    return (T) this;
  }

  @Override
  public T withAADPrefix(ByteBuffer aadPrefix) {
    this.fileAADPrefix = aadPrefix;
    return (T) this;
  }

  protected InputFile file() {
    return file;
  }

  protected Map<String, String> properties() {
    return properties;
  }

  protected Long start() {
    return start;
  }

  protected Long length() {
    return length;
  }

  protected Schema schema() {
    return schema;
  }

  protected Expression filter() {
    return filter;
  }

  protected boolean isFilterRecords() {
    return filterRecords;
  }

  protected boolean isCaseSensitive() {
    return caseSensitive;
  }

  protected boolean isReuseContainers() {
    return reuseContainers;
  }

  protected int recordsPerBatch() {
    return recordsPerBatch;
  }

  protected NameMapping nameMapping() {
    return nameMapping;
  }

  protected ByteBuffer fileEncryptionKey() {
    return fileEncryptionKey;
  }

  protected ByteBuffer fileAADPrefix() {
    return fileAADPrefix;
  }
}
