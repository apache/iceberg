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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataGenerator;
import org.apache.iceberg.data.DataGenerators;
import org.apache.iceberg.data.ReadFormatModelTests;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.data.vectorized.IcebergArrowColumnVector;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;

class TestSparkVectorizedFormatModel extends ReadFormatModelTests<ColumnarBatch> {

  private static final FileFormat[] FILE_FORMATS = {FileFormat.ORC, FileFormat.PARQUET};

  // Spark has no TIME or TIMESTAMP_NANO type. The Arrow-based Parquet reader also cannot read
  // nested or geospatial columns, the vectorized ORC reader can.
  private static final Map<FileFormat, Set<Type.TypeID>> UNSUPPORTED_TYPE_IDS =
      Map.of(
          FileFormat.ORC,
          Set.of(Type.TypeID.TIME, Type.TypeID.TIMESTAMP_NANO),
          FileFormat.PARQUET,
          Set.of(
              Type.TypeID.TIME,
              Type.TypeID.TIMESTAMP_NANO,
              Type.TypeID.STRUCT,
              Type.TypeID.LIST,
              Type.TypeID.MAP,
              Type.TypeID.GEOMETRY,
              Type.TypeID.GEOGRAPHY));

  private static final List<Arguments> FORMAT_AND_GENERATOR =
      Arrays.stream(FILE_FORMATS)
          .flatMap(
              format ->
                  Arrays.stream(DataGenerators.ALL)
                      .filter(generator -> supportsVectorized(format, generator))
                      .map(generator -> Arguments.of(format, generator)))
          .toList();

  private static boolean supportsVectorized(FileFormat format, DataGenerator generator) {
    return supportsGenerator(format, generator)
        && !containsType(generator.schema(), UNSUPPORTED_TYPE_IDS.get(format));
  }

  @Override
  protected Collection<Type.TypeID> unsupportedTypeIds(FileFormat fileFormat) {
    return UNSUPPORTED_TYPE_IDS.get(fileFormat);
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

  @Override
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  protected void testReadMetadataColumnIsDeleted(FileFormat fileFormat) throws IOException {
    assumeThat(fileFormat)
        .as("Vectorized Parquet reader only populates _deleted when a delete filter is set")
        .isNotEqualTo(FileFormat.PARQUET);
    super.testReadMetadataColumnIsDeleted(fileFormat);
  }

  @Override
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  protected void testReadMetadataColumnRowLineage(FileFormat fileFormat) throws IOException {
    assumeRowLineageSupported(fileFormat);
    super.testReadMetadataColumnRowLineage(fileFormat);
  }

  @Override
  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  protected void testReadMetadataColumnRowLineageExistingValues(FileFormat fileFormat)
      throws IOException {
    assumeRowLineageSupported(fileFormat);
    super.testReadMetadataColumnRowLineageExistingValues(fileFormat);
  }

  // TODO: remove when https://github.com/apache/iceberg/issues/18543 is fixed
  private static void assumeRowLineageSupported(FileFormat fileFormat) {
    assumeThat(fileFormat)
        .as("Vectorized ORC reader does not apply row lineage")
        .isNotEqualTo(FileFormat.ORC);
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
