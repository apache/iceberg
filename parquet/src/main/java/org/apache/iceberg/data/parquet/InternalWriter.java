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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.ParquetValueWriters.PrimitiveWriter;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.iceberg.types.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

/**
 * A Writer that consumes Iceberg's internal in-memory object model.
 *
 * <p>Iceberg's internal in-memory object model produces the types defined in {@link
 * Type.TypeID#javaClass()}.
 */
public class InternalWriter extends BaseParquetWriter<StructLike> {
  private static final InternalWriter INSTANCE = new InternalWriter();

  private InternalWriter() {}

  public static ParquetValueWriter<StructLike> create(MessageType type) {
    return INSTANCE.createWriter(type);
  }

  @Override
  protected StructWriter<StructLike> createStructWriter(List<ParquetValueWriter<?>> writers) {
    return new ParquetValueWriters.RecordWriter<>(writers);
  }

  @Override
  protected PrimitiveWriter<?> fixedWriter(ColumnDescriptor desc) {
    return ParquetValueWriters.fixedBuffer(desc);
  }

  @Override
  protected PrimitiveWriter<?> dateWriter(ColumnDescriptor desc) {
    // Use primitive-type writer; no special writer needed.
    return null;
  }

  @Override
  protected PrimitiveWriter<?> timeWriter(ColumnDescriptor desc) {
    // Use primitive-type writer; no special writer needed.
    return null;
  }

  @Override
  protected PrimitiveWriter<?> timestampWriter(ColumnDescriptor desc, boolean isAdjustedToUTC) {
    // Use primitive-type writer; no special writer needed.
    return null;
  }

  @Override
  protected PrimitiveWriter<?> uuidWriter(ColumnDescriptor desc) {
    return ParquetValueWriters.uuids(desc);
  }
}
