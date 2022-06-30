/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.UnionColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;

public class TestSparkOrcUnions {
    private static final int NUM_OF_ROWS = 50;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testComplexUnion() throws IOException {
        TypeDescription orcSchema =
                TypeDescription.fromString("struct<unionCol:uniontype<int,string>>");

        Schema expectedSchema = new Schema(
                Types.NestedField.optional(0, "unionCol", Types.StructType.of(
                        Types.NestedField.optional(3, "tag", Types.IntegerType.get()),
                        Types.NestedField.optional(1, "field0", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "field1", Types.StringType.get())))
        );

        final InternalRow expectedFirstRow = new GenericInternalRow(1);
        final InternalRow field1 = new GenericInternalRow(3);
        field1.update(0, 0);
        field1.update(1, 0);
        field1.update(2, null);
        expectedFirstRow.update(0, field1);

        final InternalRow expectedSecondRow = new GenericInternalRow(1);
        final InternalRow field2 = new GenericInternalRow(3);
        field2.update(0, 1);
        field2.update(1, null);
        field2.update(2, UTF8String.fromString("foo-1"));
        expectedSecondRow.update(0, field2);

        Configuration conf = new Configuration();

        File orcFile = temp.newFile();
        Path orcFilePath = new Path(orcFile.getPath());

        Writer writer = OrcFile.createWriter(orcFilePath,
                OrcFile.writerOptions(conf)
                        .setSchema(orcSchema).overwrite(true));

        VectorizedRowBatch batch = orcSchema.createRowBatch();
        LongColumnVector longColumnVector = new LongColumnVector(NUM_OF_ROWS);
        BytesColumnVector bytesColumnVector = new BytesColumnVector(NUM_OF_ROWS);
        UnionColumnVector complexUnion = new UnionColumnVector(NUM_OF_ROWS, longColumnVector, bytesColumnVector);

        complexUnion.init();

        for (int i = 0; i < NUM_OF_ROWS; i += 1) {
            complexUnion.tags[i] = i % 2;
            longColumnVector.vector[i] = i;
            String stringValue = "foo-" + i;
            bytesColumnVector.setVal(i, stringValue.getBytes(StandardCharsets.UTF_8));
        }

        batch.size = NUM_OF_ROWS;
        batch.cols[0] = complexUnion;

        writer.addRowBatch(batch);
        batch.reset();
        writer.close();

        // Test non-vectorized reader
        List<InternalRow> actualRows = Lists.newArrayList();
        try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(orcFile))
                .project(expectedSchema)
                .createReaderFunc(readOrcSchema -> new SparkOrcReader(expectedSchema, readOrcSchema))
                .build()) {
            reader.forEach(actualRows::add);

            Assert.assertEquals(actualRows.size(), NUM_OF_ROWS);
            assertEquals(expectedSchema, expectedFirstRow, actualRows.get(0));
            assertEquals(expectedSchema, expectedSecondRow, actualRows.get(1));
        }

        // Test vectorized reader
        try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
                .project(expectedSchema)
                .createBatchedReaderFunc(readOrcSchema ->
                        VectorizedSparkOrcReaders.buildReader(expectedSchema, readOrcSchema, ImmutableMap.of()))
                .build()) {
            final Iterator<InternalRow> actualRowsIt = batchesToRows(reader.iterator());

            assertEquals(expectedSchema, expectedFirstRow, actualRowsIt.next());
            assertEquals(expectedSchema, expectedSecondRow, actualRowsIt.next());
        }
    }

    @Test
    public void testDeeplyNestedUnion() throws IOException {
        TypeDescription orcSchema =
                TypeDescription.fromString("struct<c1:uniontype<int,struct<c2:string,c3:uniontype<int,string>>>>");

        Schema expectedSchema = new Schema(
                Types.NestedField.optional(0, "c1", Types.StructType.of(
                        Types.NestedField.optional(100, "tag", Types.IntegerType.get()),
                        Types.NestedField.optional(1, "field0", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "field1",
                                Types.StructType.of(Types.NestedField.optional(3, "c2", Types.StringType.get()),
                                        Types.NestedField.optional(4, "c3", Types.StructType.of(
                                                Types.NestedField.optional(101, "tag", Types.IntegerType.get()),
                                                Types.NestedField.optional(5, "field0", Types.IntegerType.get()),
                                                Types.NestedField.optional(6, "field1", Types.StringType.get()))))))));

        final InternalRow expectedFirstRow = new GenericInternalRow(1);
        final InternalRow inner1 = new GenericInternalRow(3);
        inner1.update(0, 1);
        inner1.update(1, null);
        final InternalRow inner2 = new GenericInternalRow(2);
        inner2.update(0, UTF8String.fromString("foo0"));
        final InternalRow inner3 = new GenericInternalRow(3);
        inner3.update(0, 0);
        inner3.update(1, 0);
        inner3.update(2, null);
        inner2.update(1, inner3);
        inner1.update(2, inner2);
        expectedFirstRow.update(0, inner1);

        Configuration conf = new Configuration();

        File orcFile = temp.newFile();
        Path orcFilePath = new Path(orcFile.getPath());

        Writer writer = OrcFile.createWriter(orcFilePath,
                OrcFile.writerOptions(conf)
                        .setSchema(orcSchema).overwrite(true));

        VectorizedRowBatch batch = orcSchema.createRowBatch();
        UnionColumnVector innerUnion1 = (UnionColumnVector) batch.cols[0];
        LongColumnVector innerInt1 = (LongColumnVector) innerUnion1.fields[0];
        innerInt1.fillWithNulls();
        StructColumnVector innerStruct2 = (StructColumnVector) innerUnion1.fields[1];
        BytesColumnVector innerString2 = (BytesColumnVector) innerStruct2.fields[0];
        UnionColumnVector innerUnion3 = (UnionColumnVector) innerStruct2.fields[1];
        LongColumnVector innerInt3 = (LongColumnVector) innerUnion3.fields[0];
        BytesColumnVector innerString3 = (BytesColumnVector) innerUnion3.fields[1];
        innerString3.fillWithNulls();

        for (int r = 0; r < NUM_OF_ROWS; ++r) {
            int row = batch.size++;
            innerUnion1.tags[row] = 1;
            innerString2.setVal(row, ("foo" + row).getBytes(StandardCharsets.UTF_8));
            innerUnion3.tags[row] = 0;
            innerInt3.vector[row] = r;
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
                innerInt1.fillWithNulls();
                innerString3.fillWithNulls();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();

        // test non-vectorized reader
        List<InternalRow> results = Lists.newArrayList();
        try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(orcFile))
                .project(expectedSchema)
                .createReaderFunc(readOrcSchema -> new SparkOrcReader(expectedSchema, readOrcSchema))
                .build()) {
            reader.forEach(results::add);
            final InternalRow actualFirstRow = results.get(0);

            Assert.assertEquals(results.size(), NUM_OF_ROWS);
            assertEquals(expectedSchema, expectedFirstRow, actualFirstRow);
        }

        // test vectorized reader
        try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
                .project(expectedSchema)
                .createBatchedReaderFunc(readOrcSchema ->
                        VectorizedSparkOrcReaders.buildReader(expectedSchema, readOrcSchema, ImmutableMap.of()))
                .build()) {
            final Iterator<InternalRow> actualRowsIt = batchesToRows(reader.iterator());

            assertEquals(expectedSchema, expectedFirstRow, actualRowsIt.next());
        }
    }

    private Iterator<InternalRow> batchesToRows(Iterator<ColumnarBatch> batches) {
        return Iterators.concat(Iterators.transform(batches, ColumnarBatch::rowIterator));
    }
}
