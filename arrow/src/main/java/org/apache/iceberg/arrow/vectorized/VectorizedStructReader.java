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
package org.apache.iceberg.arrow.vectorized;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * A vectorized reader for struct (nested) fields that reads child columns individually and
 * assembles them into an Arrow {@link StructVector}.
 */
public class VectorizedStructReader extends VectorizedArrowReader {

  private final VectorizedReader<VectorHolder>[] childReaders;
  private final Types.StructType structType;
  private final BufferAllocator allocator;

  private StructVector structVector;
  private NullabilityHolder nullabilityHolder;

  @SuppressWarnings("unchecked")
  VectorizedStructReader(
      Types.NestedField icebergField, List<VectorizedReader<?>> readers, BufferAllocator allocator) {
    super(icebergField);
    this.structType = icebergField.type().asStructType();
    this.childReaders = readers.toArray(new VectorizedReader[0]);
    this.allocator = allocator.newChildAllocator("struct-" + icebergField.name(), 0, Long.MAX_VALUE);
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numValsToRead) {
    // Always close the previous struct vector since child data is transferred into it
    // and cannot be reused after transfer
    if (structVector != null) {
      structVector.close();
      structVector = null;
    }

    List<Types.NestedField> fields = structType.fields();
    FieldVector[] childVectors = new FieldVector[childReaders.length];

    for (int i = 0; i < childReaders.length; i++) {
      VectorHolder childHolder = childReaders[i].read(null, numValsToRead);
      childVectors[i] = childHolder.vector();
    }

    // Build the StructVector from child vectors
    structVector = StructVector.empty(icebergField().name(), allocator);

    // Add child vectors
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField childField = fields.get(i);
      Field childArrowField = ArrowSchemaUtil.convert(childField);
      FieldVector child = childVectors[i];
      if (child != null) {
        structVector.addOrGet(
            childArrowField.getName(),
            childArrowField.getFieldType(),
            child.getClass());
        // Transfer data from the reader's child vector into the struct's child vector
        FieldVector structChild = structVector.getChild(childArrowField.getName());
        child.makeTransferPair(structChild).transfer();
      }
    }

    structVector.setValueCount(numValsToRead);
    // Set all values as non-null at the struct level by default.
    // Individual child nullability is handled by the child readers.
    if (nullabilityHolder == null || nullabilityHolder.size() < numValsToRead) {
      nullabilityHolder = new NullabilityHolder(numValsToRead);
    } else {
      nullabilityHolder.reset();
    }

    nullabilityHolder.setNotNulls(0, numValsToRead);

    return new VectorHolder(structVector, icebergField(), nullabilityHolder);
  }

  @Override
  public void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    for (VectorizedReader<VectorHolder> reader : childReaders) {
      if (reader != null) {
        reader.setRowGroupInfo(source, metadata);
      }
    }
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (VectorizedReader<VectorHolder> reader : childReaders) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }

  @Override
  public void close() {
    for (VectorizedReader<VectorHolder> reader : childReaders) {
      if (reader != null) {
        reader.close();
      }
    }

    if (structVector != null) {
      structVector.close();
      structVector = null;
    }

    allocator.close();
  }

  @Override
  public String toString() {
    return String.format("VectorizedStructReader: %s", icebergField().name());
  }
}
