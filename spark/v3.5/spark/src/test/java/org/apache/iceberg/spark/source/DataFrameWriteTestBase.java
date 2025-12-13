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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.spark.SparkSchemaUtil.convert;
import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkPlannedAvroReader;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class DataFrameWriteTestBase extends ScanTestBase {
  @TempDir private Path temp;

  @Override
  protected boolean supportsDefaultValues() {
    // disable default value tests because this tests the write path
    return false;
  }

  @Override
  protected void writeRecords(Table table, List<GenericData.Record> records) throws IOException {
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    Dataset<Row> df = createDataset(records, tableSchema);
    DataFrameWriter<?> writer = df.write().format("iceberg").mode("append");

    writer.save(table.location());

    // refresh the in-memory table state to pick up Spark's write
    table.refresh();
  }

  private Dataset<Row> createDataset(List<GenericData.Record> records, Schema schema)
      throws IOException {
    // this uses the SparkAvroReader to create a DataFrame from the list of records
    // it assumes that SparkAvroReader is correct
    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<GenericData.Record> writer =
        Avro.write(Files.localOutput(testFile)).schema(schema).named("test").build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader =
        Avro.read(Files.localInput(testFile))
            .createResolvingReader(SparkPlannedAvroReader::create)
            .project(schema)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    // verify that the dataframe matches
    assertThat(rows).hasSameSizeAs(records);
    Iterator<GenericData.Record> recordIter = records.iterator();
    for (InternalRow row : rows) {
      assertEqualsUnsafe(schema.asStruct(), recordIter.next(), row);
    }

    JavaRDD<InternalRow> rdd = sc.parallelize(rows);
    return spark.internalCreateDataFrame(JavaRDD.toRDD(rdd), convert(schema), false);
  }

  @Test
  public void testAlternateLocation() throws IOException {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));

    File location = temp.resolve("table_location").toFile();
    File altLocation = temp.resolve("alt_location").toFile();

    Tables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(schema, PartitionSpec.unpartitioned(), location.toString());

    // override the table's data location
    table
        .updateProperties()
        .set(TableProperties.WRITE_DATA_LOCATION, altLocation.getAbsolutePath())
        .commit();

    writeRecords(table, RandomData.generateList(table.schema(), 100, 87112L));

    table
        .currentSnapshot()
        .addedDataFiles(table.io())
        .forEach(
            dataFile ->
                assertThat(dataFile.location())
                    .as(
                        String.format(
                            "File should have the parent directory %s, but has: %s.",
                            altLocation, dataFile.location()))
                    .startsWith(altLocation + "/"));
  }
}
