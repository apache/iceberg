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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTimestampPartitions extends TestBase {

  @TestTemplate
  public void testPartitionAppend() throws IOException {
    Schema dateSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "timestamp", Types.TimestampType.withoutZone()));

    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(dateSchema).day("timestamp", "date").build();

    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath("/path/to/data-1.parquet")
            .withFileSizeInBytes(0)
            .withRecordCount(0)
            .withPartitionPath("date=2018-06-08")
            .build();

    this.table =
        TestTables.create(
            tableDir, "test_date_partition", dateSchema, partitionSpec, formatVersion);

    table.newAppend().appendFile(dataFile).commit();
    long id = table.currentSnapshot().snapshotId();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    validateManifestEntries(
        table.currentSnapshot().allManifests(table.io()).get(0),
        ids(id),
        files(dataFile),
        statuses(ManifestEntry.Status.ADDED));
  }

  // identity() keeps the source column type in the partition tuple, unlike day()/hour();
  // this is the case where PartitionData/GenericDataFile/GenericDeleteFile must encode
  // TimestampType.withoutZone() as spec-mandated timestamp-micros, not local-timestamp-micros.

  @TestTemplate
  public void testIdentityPartitionAvroSchemaIsSpecCompliant() {
    PartitionSpec spec = identityTimestampSpec();
    assertTimestampFieldIsSpecCompliant(PartitionData.partitionDataSchema(spec.partitionType()));
  }

  @TestTemplate
  public void testIdentityPartitionDataFileAvroSchemaIsSpecCompliant() {
    PartitionSpec spec = identityTimestampSpec();
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-1.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionWithTimestamp(spec))
            .build();

    assertTimestampFieldIsSpecCompliant(
        unwrapOption(((IndexedRecord) dataFile).getSchema().getField("partition").schema()));
  }

  @TestTemplate
  public void testIdentityPartitionDeleteFileAvroSchemaIsSpecCompliant() {
    PartitionSpec spec = identityTimestampSpec();
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath("/path/to/delete-1.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionWithTimestamp(spec))
            .build();

    assertTimestampFieldIsSpecCompliant(
        unwrapOption(((IndexedRecord) deleteFile).getSchema().getField("partition").schema()));
  }

  @TestTemplate
  public void testIdentityPartitionDataFileProjectionAvroSchemaIsSpecCompliant() {
    PartitionSpec spec = identityTimestampSpec();
    GenericDataFile dataFile = new GenericDataFile(DataFile.getType(spec.partitionType()));

    assertTimestampFieldIsSpecCompliant(
        unwrapOption(dataFile.getSchema().getField("partition").schema()));
  }

  @TestTemplate
  public void testIdentityPartitionManifestEntryAvroSchemaIsSpecCompliant() {
    PartitionSpec spec = identityTimestampSpec();
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(V1Metadata.entrySchema(spec.partitionType()).asStruct());

    assertTimestampFieldIsSpecCompliant(
        unwrapOption(
            entry.getSchema().getField("data_file").schema().getField("partition").schema()));
  }

  // adjust-to-utc defaults to false when absent, so a spec-compliant writer may omit it for
  // withoutZone() columns. Reading such a schema must still produce TimestampType.withoutZone().

  @TestTemplate
  public void testPartitionDataReadFromSchemaWithoutAdjustToUtcProp() {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("r")
            .fields()
            .name("ts")
            .type(
                LogicalTypes.timestampMicros()
                    .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)))
            .noDefault()
            .endRecord();
    avroSchema.getField("ts").addProp("field-id", 1000);

    PartitionData data = new PartitionData(avroSchema);

    assertThat(data.getPartitionType().fields().get(0).type())
        .isEqualTo(Types.TimestampType.withoutZone());
  }

  @TestTemplate
  public void testDataFileReadFromSchemaWithoutAdjustToUtcProp() {
    PartitionSpec spec = identityTimestampSpec();
    GenericDataFile written = new GenericDataFile(DataFile.getType(spec.partitionType()));
    GenericDataFile reconstructed = new GenericDataFile(stripAdjustToUtcProp(written.getSchema()));
    PartitionData partitionData = (PartitionData) reconstructed.partition();

    assertThat(partitionData.getPartitionType().fields().get(0).type())
        .isEqualTo(Types.TimestampType.withoutZone());
  }

  private PartitionSpec identityTimestampSpec() {
    Schema tsSchema = new Schema(required(1, "ts", Types.TimestampType.withoutZone()));
    return PartitionSpec.builderFor(tsSchema).identity("ts").build();
  }

  private PartitionData partitionWithTimestamp(PartitionSpec spec) {
    PartitionData data = new PartitionData(spec.partitionType());
    data.set(0, 1528416000000000L);
    return data;
  }

  private void assertTimestampFieldIsSpecCompliant(org.apache.avro.Schema partitionSchema) {
    org.apache.avro.Schema tsFieldSchema = unwrapOption(partitionSchema.getField("ts").schema());
    assertThat(tsFieldSchema.getLogicalType().getName()).isEqualTo("timestamp-micros");
    assertThat(tsFieldSchema.getObjectProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP)).isEqualTo(false);
  }

  private static org.apache.avro.Schema unwrapOption(org.apache.avro.Schema schema) {
    if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
      return schema;
    }
    return schema.getTypes().stream()
        .filter(type -> type.getType() != org.apache.avro.Schema.Type.NULL)
        .findFirst()
        .orElseThrow();
  }

  private static org.apache.avro.Schema stripAdjustToUtcProp(org.apache.avro.Schema schema) {
    String json = schema.toString().replaceAll(",\"adjust-to-utc\":(true|false)", "");
    return new org.apache.avro.Schema.Parser().parse(json);
  }
}
