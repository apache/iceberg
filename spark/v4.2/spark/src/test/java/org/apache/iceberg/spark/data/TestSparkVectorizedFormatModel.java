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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataGenerators;
import org.apache.iceberg.data.ReadFormatModelTests;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.data.vectorized.IcebergArrowColumnVector;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.params.provider.Arguments;

class TestSparkVectorizedFormatModel extends ReadFormatModelTests<ColumnarBatch> {

  private static final FileFormat[] FILE_FORMATS = {FileFormat.ORC, FileFormat.PARQUET};

  private static final List<Arguments> FORMAT_AND_GENERATOR =
      Arrays.stream(FILE_FORMATS)
          .flatMap(
              format ->
                  Arrays.stream(DataGenerators.ALL)
                      .filter(generator -> supportsGenerator(format, generator))
                      .map(generator -> Arguments.of(format, generator)))
          .toList();

  private static final Set<Type.TypeID> UNSUPPORTED_TYPE_IDS =
      Set.of(
          Type.TypeID.TIME,
          Type.TypeID.TIMESTAMP_NANO,
          Type.TypeID.STRUCT,
          Type.TypeID.LIST,
          Type.TypeID.MAP,
          Type.TypeID.UUID,
          Type.TypeID.GEOMETRY,
          Type.TypeID.GEOGRAPHY);

  @Override
  protected Collection<Type.TypeID> unsupportedTypeIds() {
    return UNSUPPORTED_TYPE_IDS;
  }

  @Override
  protected boolean supportsBatchReads() {
    return true;
  }

  @Override
  protected int countRows(List<ColumnarBatch> batches) {
    return batches.stream().mapToInt(ColumnarBatch::numRows).sum();
  }

  @Override
  protected void assertContainersReused(ColumnarBatch first, ColumnarBatch second) {
    assertThat(second.numCols()).isEqualTo(first.numCols());

    for (int i = 0; i < first.numCols(); i++) {
      assertThat(arrowVector(second, i)).isSameAs(arrowVector(first, i));
    }
  }

  private ValueVector arrowVector(ColumnarBatch batch, int col) {
    assertThat(batch.column(col)).isInstanceOf(IcebergArrowColumnVector.class);

    IcebergArrowColumnVector vector = (IcebergArrowColumnVector) batch.column(col);
    return vector.vectorAccessor().getVector();
  }

  @Override
  protected void assertContainersNotReused(ColumnarBatch first, ColumnarBatch second) {
    assertThat(second.numCols()).isEqualTo(first.numCols());

    for (int i = 0; i < first.numCols(); i++) {
      assertThat(arrowVector(second, i)).isNotSameAs(arrowVector(first, i));
    }
  }

  @Override
  protected Object engineSchema(Schema schema) {
    return SparkSchemaUtil.convert(schema);
  }

  @Override
  protected Object convertConstantToEngine(Type type, Object value) {
    return SparkUtil.internalToSpark(type, value);
  }

  @Override
  protected Class<ColumnarBatch> engineType() {
    return ColumnarBatch.class;
  }

  @Override
  protected ColumnarBatch convertToEngine(Record record, Schema schema) {
    throw new UnsupportedOperationException("Vectorized reads are read-only");
  }

  @Override
  protected void assertEquals(
      Schema schema, List<ColumnarBatch> expected, List<ColumnarBatch> actual) {
    throw new UnsupportedOperationException("Use assertRecordsEqual for batch reads");
  }

  @Override
  protected void assertRecordsEqual(
      Schema schema, List<Record> expected, CloseableIterable<ColumnarBatch> actual) {
    int rowOffset = 0;

    for (ColumnarBatch batch : actual) {
      assertThat(rowOffset + batch.numRows()).isLessThanOrEqualTo(expected.size());

      for (int i = 0; i < batch.numRows(); i++) {
        TestHelpers.assertEquals(
            schema,
            InternalRowConverter.convert(schema, expected.get(rowOffset + i)),
            batch.getRow(i));
      }
      rowOffset += batch.numRows();
    }

    assertThat(rowOffset).isEqualTo(expected.size());
  }
}
