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
package org.apache.iceberg.data.parquet;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

/**
 * A Writer that consumes Iceberg's internal in-memory object model.
 *
 * <p>Iceberg's internal in-memory object model produces the types defined in {@link
 * Type.TypeID#javaClass()}.
 */
public class InternalWriter<T extends StructLike> extends BaseParquetWriter<T> {
  private static final InternalWriter<?> INSTANCE = new InternalWriter<>();

  private InternalWriter() {}

  public static <T extends StructLike> ParquetValueWriter<T> createWriter(
      Schema schema, MessageType type) {
    return create(schema.asStruct(), type);
  }

  @SuppressWarnings("unchecked")
  public static <T extends StructLike> ParquetValueWriter<T> create(
      Types.StructType struct, MessageType type) {
    return (ParquetValueWriter<T>) INSTANCE.createWriter(struct, type);
  }

  @Override
  protected StructWriter<T> createStructWriter(
      Types.StructType struct, List<ParquetValueWriter<?>> writers) {
    return ParquetValueWriters.recordWriter(struct, writers);
  }

  @Override
  protected ParquetValueWriter<?> fixedWriter(ColumnDescriptor desc) {
    return ParquetValueWriters.fixedBuffers(desc);
  }

  @Override
  protected ParquetValueWriter<?> dateWriter(ColumnDescriptor desc) {
    return ParquetValueWriters.ints(desc);
  }

  @Override
  protected ParquetValueWriter<?> timeWriter(ColumnDescriptor desc) {
    return ParquetValueWriters.longs(desc);
  }

  @Override
  protected ParquetValueWriter<?> timestampWriter(ColumnDescriptor desc, boolean isAdjustedToUTC) {
    return ParquetValueWriters.longs(desc);
  }
}
