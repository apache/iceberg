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
package org.apache.iceberg;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructProjection;

class StaticDataTask implements DataTask {

  static <T> DataTask of(
      InputFile metadata,
      Schema tableSchema,
      Schema projectedSchema,
      Iterable<T> values,
      Function<T, Row> transform) {
    return new StaticDataTask(
        metadata,
        tableSchema,
        projectedSchema,
        Lists.newArrayList(Iterables.transform(values, transform::apply)).toArray(new Row[0]));
  }

  private final DataFile metadataFile;
  private final StructLike[] rows;
  private final Schema tableSchema;
  private final Schema projectedSchema;

  private StaticDataTask(
      InputFile metadata, Schema tableSchema, Schema projectedSchema, StructLike[] rows) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.metadataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(metadata)
            .withRecordCount(rows.length)
            .withFormat(FileFormat.METADATA)
            .build();
    this.rows = rows;
  }

  StaticDataTask(
      DataFile metadataFile, Schema tableSchema, Schema projectedSchema, StructLike[] rows) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.metadataFile = metadataFile;
    this.rows = rows;
  }

  @Override
  public Schema schema() {
    return tableSchema;
  }

  @Override
  public List<DeleteFile> deletes() {
    return ImmutableList.of();
  }

  @Override
  public CloseableIterable<StructLike> rows() {
    StructProjection projection = StructProjection.create(tableSchema, projectedSchema);
    Iterable<StructLike> projectedRows = Iterables.transform(Arrays.asList(rows), projection::wrap);
    return CloseableIterable.withNoopClose(projectedRows);
  }

  @Override
  public DataFile file() {
    return metadataFile;
  }

  @Override
  public PartitionSpec spec() {
    return PartitionSpec.unpartitioned();
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return metadataFile.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return Expressions.alwaysTrue();
  }

  @Override
  public Iterable<FileScanTask> split(long splitSize) {
    return ImmutableList.of(this);
  }

  Schema projectedSchema() {
    return projectedSchema;
  }

  DataFile metadataFile() {
    return metadataFile;
  }

  /** @return the table rows before projection */
  StructLike[] tableRows() {
    return rows;
  }

  /** Implements {@link StructLike#get} for passing static rows. */
  static class Row implements StructLike, Serializable {
    public static Row of(Object... values) {
      return new Row(values);
    }

    private final Object[] values;

    private Row(Object... values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Setting values is not supported");
    }
  }
}
