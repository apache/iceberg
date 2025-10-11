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

import static org.apache.iceberg.TestHelpers.ALL_VERSIONS;
import static org.apache.iceberg.TestHelpers.V3_AND_ABOVE;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.RandomAvroData;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestScansAndSchemaEvolution {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "part", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("part").build();

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return ALL_VERSIONS;
  }

  @Parameter private int formatVersion;

  @TempDir private File temp;

  private DataFile createDataFile(String partValue) throws IOException {
    return createDataFile(partValue, SCHEMA, SPEC);
  }

  private DataFile createDataFile(String partValue, Schema schema, PartitionSpec spec)
      throws IOException {
    List<GenericData.Record> expected = RandomAvroData.generate(schema, 100, 0L);

    OutputFile dataFile =
        new InMemoryOutputFile(FileFormat.AVRO.addExtension(UUID.randomUUID().toString()));
    try (FileAppender<GenericData.Record> writer =
        Avro.write(dataFile).schema(schema).named("test").build()) {
      for (GenericData.Record rec : expected) {
        rec.put("part", partValue); // create just one partition
        writer.add(rec);
      }
    }

    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, partValue);
    return DataFiles.builder(spec)
        .withInputFile(dataFile.toInputFile())
        .withPartition(partition)
        .withRecordCount(100)
        .build();
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void testPartitionSourceRename() throws IOException {
    Table table = TestTables.create(temp, "test", SCHEMA, SPEC, formatVersion);

    DataFile fileOne = createDataFile("one");
    DataFile fileTwo = createDataFile("two");

    table.newAppend().appendFile(fileOne).appendFile(fileTwo).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    List<FileScanTask> tasks =
        Lists.newArrayList(table.newScan().filter(Expressions.equal("part", "one")).planFiles());

    assertThat(tasks).hasSize(1);

    table.updateSchema().renameColumn("part", "p").commit();

    // plan the scan using the new name in a filter
    tasks = Lists.newArrayList(table.newScan().filter(Expressions.equal("p", "one")).planFiles());

    assertThat(tasks).hasSize(1);

    // create a new commit
    table.newAppend().appendFile(createDataFile("three")).commit();

    // use fiter with previous partition name
    tasks =
        Lists.newArrayList(
            table
                .newScan()
                .useSnapshot(firstSnapshotId)
                .filter(Expressions.equal("part", "one"))
                .planFiles());

    assertThat(tasks).hasSize(1);
  }

  @TestTemplate
  public void testPartitionSourceDrop() throws IOException {
    Table table = TestTables.create(temp, "test", SCHEMA, SPEC, formatVersion);

    DataFile fileOne = createDataFile("one");
    DataFile fileTwo = createDataFile("two");

    table.newAppend().appendFile(fileOne).appendFile(fileTwo).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    table.updateSpec().addField("id").commit();

    List<FileScanTask> tasks =
        Lists.newArrayList(
            table.newScan().filter(Expressions.not(Expressions.isNull("id"))).planFiles());

    assertThat(tasks).hasSize(2);

    DataFile fileThree = createDataFile("three", table.schema(), table.spec());
    table.newAppend().appendFile(fileThree).commit();

    // remove one field from spec and drop the column
    table.updateSpec().removeField("id").commit();
    table.updateSchema().deleteColumn("id").commit();

    List<FileScanTask> tasksAtFirstSnapshotId =
        Lists.newArrayList(
            table
                .newScan()
                .useSnapshot(firstSnapshotId)
                .filter(Expressions.not(Expressions.isNull("id")))
                .planFiles());

    assertThat(
            tasksAtFirstSnapshotId.stream()
                .map(ContentScanTask::file)
                .map(ContentFile::location)
                .collect(Collectors.toList()))
        .isEqualTo(
            tasks.stream()
                .map(ContentScanTask::file)
                .map(ContentFile::location)
                .collect(Collectors.toList()));
  }

  @TestTemplate
  public void testColumnRename() throws IOException {
    Table table = TestTables.create(temp, "test", SCHEMA, SPEC, formatVersion);

    DataFile fileOne = createDataFile("one");
    DataFile fileTwo = createDataFile("two");

    table.newAppend().appendFile(fileOne).appendFile(fileTwo).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    table.updateSchema().renameColumn("data", "renamed_data").commit();

    DataFile fileThree = createDataFile("three", table.schema(), table.spec());
    table.newAppend().appendFile(fileThree).commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // generate a new commit
    DataFile fileFour = createDataFile("four", table.schema(), table.spec());
    table.newAppend().appendFile(fileFour).commit();

    // running successfully with the new filter on previous column name
    List<FileScanTask> tasks =
        Lists.newArrayList(
            table
                .newScan()
                .useSnapshot(firstSnapshotId)
                .filter(Expressions.equal("data", "xyz"))
                .planFiles());
    assertThat(tasks).hasSize(2);

    // running successfully with the new filter on renamed column name
    tasks =
        Lists.newArrayList(
            table
                .newScan()
                .useSnapshot(secondSnapshotId)
                .filter(Expressions.equal("renamed_data", "xyz"))
                .planFiles());
    assertThat(tasks).hasSize(3);
  }

  @TestTemplate
  public void testColumnDrop() throws IOException {
    Table table = TestTables.create(temp, "test", SCHEMA, SPEC, formatVersion);

    DataFile fileOne = createDataFile("one");
    DataFile fileTwo = createDataFile("two");

    table.newAppend().appendFile(fileOne).appendFile(fileTwo).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    table.updateSchema().deleteColumn("data").commit();

    // make sure generating a new commit after dropping a column
    DataFile fileThree = createDataFile("three", table.schema(), table.spec());
    table.newAppend().appendFile(fileThree).commit();

    // running successfully with the new filter on previous column name
    List<FileScanTask> tasks =
        Lists.newArrayList(
            table
                .newScan()
                .useSnapshot(firstSnapshotId)
                .filter(Expressions.equal("data", "xyz"))
                .planFiles());
    assertThat(tasks).hasSize(2);
  }

  @TestTemplate
  public void testAddColumnWithDefaultValueAndQuery() throws IOException {
    assumeThat(V3_AND_ABOVE).as("Default values require v3+").contains(formatVersion);
    Table table = TestTables.create(temp, "test", SCHEMA, SPEC, formatVersion);

    // Write initial data
    DataFile fileOne = createDataFile("one");
    DataFile fileTwo = createDataFile("two");
    table.newAppend().appendFile(fileOne).appendFile(fileTwo).commit();

    // Add a new column with an initial default value
    String defaultValue = "default_category";
    table
        .updateSchema()
        .addColumn("category", Types.StringType.get(), "Product category", Literal.of(defaultValue))
        .commit();

    // Verify the schema includes the new column with default value
    Schema updatedSchema = table.schema();
    Types.NestedField categoryField = updatedSchema.findField("category");
    assertThat(categoryField).isNotNull();
    assertThat(categoryField.initialDefault()).isEqualTo(defaultValue);
    assertThat(categoryField.writeDefault()).isEqualTo(defaultValue);

    // Verify scan planning works with the new column that has default value
    assertThat(table.newScan().planFiles()).hasSize(2);

    // Test that scan with projection includes the new column with default value
    Schema projectionSchema = table.schema().select("id", "data", "category");
    assertThat(table.newScan().project(projectionSchema).planFiles())
        .hasSize(2)
        .allSatisfy(
            task -> {
              assertThat(task.schema().findField("category")).isNotNull();
              assertThat(task.schema().findField("category").initialDefault())
                  .isEqualTo(defaultValue);
            });

    // Test scan with filter on the new default column
    assertThat(table.newScan().filter(Expressions.equal("category", defaultValue)).planFiles())
        .hasSize(2); // All files should match since default applies to all

    // Test scan with filter on a value that is different than default.
    assertThat(table.newScan().filter(Expressions.equal("category", "non_default")).planFiles())
        .hasSize(2); // Files are returned, filtering happens during read

    // Write new data after schema evolution
    DataFile fileThree = createDataFile("three");
    table.newAppend().appendFile(fileThree).commit();

    // Test that all tasks have access to the column with default value
    assertThat(table.newScan().planFiles())
        .hasSize(3)
        .allSatisfy(
            task -> {
              Schema taskSchema = task.schema();
              Types.NestedField categoryFieldInTask = taskSchema.findField("category");
              assertThat(categoryFieldInTask).isNotNull();
              assertThat(categoryFieldInTask.initialDefault()).isEqualTo(defaultValue);
              assertThat(categoryFieldInTask.writeDefault()).isEqualTo(defaultValue);
            });
  }

  @TestTemplate
  public void testAddColumnWithDefaultValueAndPartitionTransform() throws IOException {
    assumeThat(V3_AND_ABOVE).as("Default values require v3+").contains(formatVersion);
    Table table = TestTables.create(temp, "test", SCHEMA, SPEC, formatVersion);

    // Write initial data
    DataFile fileOne = createDataFile("one");
    DataFile fileTwo = createDataFile("two");
    table.newAppend().appendFile(fileOne).appendFile(fileTwo).commit();

    // Add a new column with an initial default value
    String defaultValue = "default_category";
    table
        .updateSchema()
        .addColumn("category", Types.StringType.get(), "Product category", Literal.of(defaultValue))
        .commit();

    // Add bucket transform on the new column with default value
    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();

    // Verify the updated partition spec includes the new column with bucket transform
    PartitionSpec updatedSpec = table.spec();
    assertThat(updatedSpec.fields())
        .hasSize(2); // original "part" (identity) + new "category_bucket_8"

    // Verify original identity partition field is preserved
    PartitionField partPartitionField = updatedSpec.fields().get(0);
    assertThat(partPartitionField.name()).isEqualTo("part");
    assertThat(partPartitionField.transform()).isEqualTo(Transforms.identity());

    // Verify new bucket partition field
    PartitionField categoryPartitionField = updatedSpec.fields().get(1);
    assertThat(categoryPartitionField.name()).isEqualTo("category_bucket_8");
    assertThat(categoryPartitionField.transform()).isEqualTo(Transforms.bucket(8));

    // Verify scan planning works with the new partition column
    assertThat(table.newScan().planFiles()).hasSize(2);

    // Test that scan with projection includes the new column with default value
    Schema projectionSchema = table.schema().select("id", "data", "category");
    assertThat(table.newScan().project(projectionSchema).planFiles())
        .hasSize(2)
        .allSatisfy(
            task -> {
              assertThat(task.schema().findField("category")).isNotNull();
              assertThat(task.schema().findField("category").initialDefault())
                  .isEqualTo(defaultValue);
            });

    // Test scan with filter on the partitioned default column
    assertThat(table.newScan().filter(Expressions.equal("category", defaultValue)).planFiles())
        .hasSize(2); // All files should match since default applies to all

    // Write new data after schema and partition evolution
    DataFile fileThree = createDataFile("three");
    table.newAppend().appendFile(fileThree).commit();

    // Test that all tasks have access to the column with default value
    assertThat(table.newScan().planFiles())
        .hasSize(3)
        .allSatisfy(
            task -> {
              Schema taskSchema = task.schema();
              Types.NestedField categoryFieldInTask = taskSchema.findField("category");
              assertThat(categoryFieldInTask).isNotNull();
              assertThat(categoryFieldInTask.initialDefault()).isEqualTo(defaultValue);
              assertThat(categoryFieldInTask.writeDefault()).isEqualTo(defaultValue);
            });
  }
}
