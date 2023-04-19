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
package org.apache.iceberg.avro;

import static org.junit.Assert.assertFalse;

import java.util.Collections;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.Schema;
import org.junit.Test;

public class TestAvroSchemaProjection {

  @Test
  public void projectWithListSchemaChanged() {
    final org.apache.avro.Schema currentAvroSchema =
        SchemaBuilder.record("myrecord")
            .namespace("unit.test")
            .fields()
            .name("f1")
            .type()
            .nullable()
            .array()
            .items(
                SchemaBuilder.record("elem")
                    .fields()
                    .name("f11")
                    .type()
                    .nullable()
                    .intType()
                    .noDefault()
                    .endRecord())
            .noDefault()
            .endRecord();

    final org.apache.avro.Schema updatedAvroSchema =
        SchemaBuilder.record("myrecord")
            .namespace("unit.test")
            .fields()
            .name("f1")
            .type()
            .nullable()
            .array()
            .items(
                SchemaBuilder.record("elem")
                    .fields()
                    .name("f11")
                    .type()
                    .nullable()
                    .intType()
                    .noDefault()
                    .name("f12")
                    .type()
                    .nullable()
                    .stringType()
                    .noDefault()
                    .endRecord())
            .noDefault()
            .endRecord();

    final Schema currentIcebergSchema = AvroSchemaUtil.toIceberg(currentAvroSchema);

    // Getting the node ID in updatedAvroSchema allocated by converting into iceberg schema and back
    final org.apache.avro.Schema idAllocatedUpdatedAvroSchema =
        AvroSchemaUtil.convert(AvroSchemaUtil.toIceberg(updatedAvroSchema).asStruct());

    final org.apache.avro.Schema projectedAvroSchema =
        AvroSchemaUtil.buildAvroProjection(
            idAllocatedUpdatedAvroSchema, currentIcebergSchema, Collections.emptyMap());

    assertFalse(
        "Result of buildAvroProjection is missing some IDs",
        AvroSchemaUtil.missingIds(projectedAvroSchema));
  }

  @Test
  public void projectWithMapSchemaChanged() {
    final org.apache.avro.Schema currentAvroSchema =
        SchemaBuilder.record("myrecord")
            .namespace("unit.test")
            .fields()
            .name("f1")
            .type()
            .nullable()
            .map()
            .values(
                SchemaBuilder.record("elem")
                    .fields()
                    .name("f11")
                    .type()
                    .nullable()
                    .intType()
                    .noDefault()
                    .endRecord())
            .noDefault()
            .endRecord();

    final org.apache.avro.Schema updatedAvroSchema =
        SchemaBuilder.record("myrecord")
            .namespace("unit.test")
            .fields()
            .name("f1")
            .type()
            .nullable()
            .map()
            .values(
                SchemaBuilder.record("elem")
                    .fields()
                    .name("f11")
                    .type()
                    .nullable()
                    .intType()
                    .noDefault()
                    .name("f12")
                    .type()
                    .nullable()
                    .stringType()
                    .noDefault()
                    .endRecord())
            .noDefault()
            .endRecord();

    final Schema currentIcebergSchema = AvroSchemaUtil.toIceberg(currentAvroSchema);

    // Getting the node ID in updatedAvroSchema allocated by converting into iceberg schema and back
    final org.apache.avro.Schema idAllocatedUpdatedAvroSchema =
        AvroSchemaUtil.convert(AvroSchemaUtil.toIceberg(updatedAvroSchema).asStruct());

    final org.apache.avro.Schema projectedAvroSchema =
        AvroSchemaUtil.buildAvroProjection(
            idAllocatedUpdatedAvroSchema, currentIcebergSchema, Collections.emptyMap());

    assertFalse(
        "Result of buildAvroProjection is missing some IDs",
        AvroSchemaUtil.missingIds(projectedAvroSchema));
  }
}
