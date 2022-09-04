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

import static org.apache.avro.generic.GenericData.Record;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestAvroNameMapping extends TestAvroReadProjection {
  @Test
  public void testMapProjections() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "location",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.optional(2, "long", Types.FloatType.get())))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("location").schema())
                    .getValueType()));
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", ImmutableMap.of("l1", location));

    // Table mapping does not project `location` map
    NameMapping nameMapping =
        MappingUtil.create(new Schema(Types.NestedField.required(0, "id", Types.LongType.get())));

    Schema readSchema = writeSchema;

    Record projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    // field id 5 comes from read schema
    Assert.assertNotNull(
        "Field missing from table mapping is renamed",
        projected.getSchema().getField("location_r5"));
    Assert.assertNull("location field should not be read", projected.get("location_r5"));
    Assert.assertEquals(34L, projected.get("id"));

    // Table mapping partially project `location` map value
    nameMapping =
        MappingUtil.create(
            new Schema(
                Types.NestedField.required(0, "id", Types.LongType.get()),
                Types.NestedField.optional(
                    5,
                    "location",
                    Types.MapType.ofOptional(
                        6,
                        7,
                        Types.StringType.get(),
                        Types.StructType.of(
                            Types.NestedField.required(1, "lat", Types.FloatType.get()))))));

    projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    Record projectedL1 = ((Map<String, Record>) projected.get("location")).get("l1");
    Assert.assertNotNull(
        "Field missing from table mapping is renamed", projectedL1.getSchema().getField("long_r2"));
    Assert.assertNull("location.value.long, should not be read", projectedL1.get("long_r2"));
  }

  @Test
  public void testComplexMapKeys() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(
                5,
                "location",
                Types.MapType.ofRequired(
                    6,
                    7,
                    Types.StructType.of(
                        Types.NestedField.required(3, "k1", Types.StringType.get()),
                        Types.NestedField.required(4, "k2", Types.StringType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.optional(2, "long", Types.FloatType.get())))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    org.apache.avro.Schema locationSchema = record.getSchema().getField("location").schema();
    Record locationElement = new Record(locationSchema.getElementType());
    Record locationKey = new Record(locationElement.getSchema().getField("key").schema());
    Record locationValue = new Record(locationElement.getSchema().getField("value").schema());

    locationKey.put("k1", "k1");
    locationKey.put("k2", "k2");
    locationValue.put("lat", 52.995143f);
    locationValue.put("long", -1.539054f);
    locationElement.put("key", locationKey);
    locationElement.put("value", locationValue);
    record.put("location", ImmutableList.of(locationElement));

    // project a subset of the map's value columns in NameMapping
    NameMapping nameMapping =
        MappingUtil.create(
            new Schema(
                Types.NestedField.required(
                    5,
                    "location",
                    Types.MapType.ofOptional(
                        6,
                        7,
                        Types.StructType.of(
                            Types.NestedField.required(3, "k1", Types.StringType.get()),
                            Types.NestedField.optional(4, "k2", Types.StringType.get())),
                        Types.StructType.of(
                            Types.NestedField.required(1, "lat", Types.FloatType.get()))))));

    Schema readSchema =
        new Schema(
            Types.NestedField.required(
                5,
                "location",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StructType.of(
                        Types.NestedField.required(3, "k1", Types.StringType.get()),
                        Types.NestedField.optional(4, "k2", Types.StringType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.optional(2, "long", Types.FloatType.get())))));

    Record projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    // The data is read back as a map
    Map<Record, Record> projectedLocation = (Map<Record, Record>) projected.get("location");
    Record projectedKey = projectedLocation.keySet().iterator().next();
    Record projectedValue = projectedLocation.values().iterator().next();
    Assert.assertEquals(
        0, Comparators.charSequences().compare("k1", (CharSequence) projectedKey.get("k1")));
    Assert.assertEquals(
        0, Comparators.charSequences().compare("k2", (CharSequence) projectedKey.get("k2")));
    Assert.assertEquals(52.995143f, projectedValue.get("lat"));
    Assert.assertNotNull(projectedValue.getSchema().getField("long_r2"));
    Assert.assertNull(projectedValue.get("long_r2"));
  }

  @Test
  public void testMissingRequiredFields() {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(19, "x", Types.IntegerType.get()),
            Types.NestedField.optional(18, "y", Types.IntegerType.get()));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("x", 1);
    record.put("y", 2);

    // table mapping not projecting a required field 'x'
    NameMapping nameMapping =
        MappingUtil.create(
            new Schema(Types.NestedField.optional(18, "y", Types.IntegerType.get())));

    Schema readSchema = writeSchema;
    AssertHelpers.assertThrows(
        "Missing required field in nameMapping",
        IllegalArgumentException.class,
        "Missing required field: x",
        // In this case, pruneColumns result is an empty record
        () -> writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping));
  }

  @Test
  public void testArrayProjections() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                22,
                "point",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.required(19, "x", Types.IntegerType.get()),
                        Types.NestedField.optional(18, "y", Types.IntegerType.get())))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record pointRecord =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("point").schema())
                    .getElementType()));
    pointRecord.put("x", 1);
    pointRecord.put("y", 2);
    record.put("point", ImmutableList.of(pointRecord));

    NameMapping nameMapping =
        MappingUtil.create(
            new Schema(
                // Optional array field missing.
                Types.NestedField.required(0, "id", Types.LongType.get())));

    Schema readSchema = writeSchema;

    Record projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    Assert.assertNotNull(
        "Field missing from table mapping is renamed", projected.getSchema().getField("point_r22"));
    Assert.assertNull("point field is not projected", projected.get("point_r22"));
    Assert.assertEquals(34L, projected.get("id"));

    // point array is partially projected
    nameMapping =
        MappingUtil.create(
            new Schema(
                Types.NestedField.required(0, "id", Types.LongType.get()),
                Types.NestedField.optional(
                    22,
                    "point",
                    Types.ListType.ofOptional(
                        21,
                        Types.StructType.of(
                            Types.NestedField.required(19, "x", Types.IntegerType.get()))))));

    projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    Record point = ((List<Record>) projected.get("point")).get(0);

    Assert.assertNotNull(
        "Field missing from table mapping is renamed", point.getSchema().getField("y_r18"));
    Assert.assertEquals("point.x is projected", 1, point.get("x"));
    Assert.assertNull("point.y is not projected", point.get("y_r18"));
    Assert.assertEquals(34L, projected.get("id"));
  }

  @Test
  public void testAliases() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.required(19, "x", Types.IntegerType.get())))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    Record pointRecord =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("points").schema())
                    .getElementType()));
    pointRecord.put("x", 1);
    record.put("points", ImmutableList.of(pointRecord));

    NameMapping nameMapping =
        NameMapping.of(
            MappedFields.of(
                MappedField.of(
                    22,
                    "points",
                    MappedFields.of(
                        MappedField.of(
                            21,
                            "element",
                            MappedFields.of(MappedField.of(19, Lists.newArrayList("x"))))))));

    Schema readSchema =
        new Schema(
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        // x renamed to y
                        Types.NestedField.required(19, "y", Types.IntegerType.get())))));

    Record projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    Assert.assertEquals(
        "x is read as y", 1, ((List<Record>) projected.get("points")).get(0).get("y"));

    readSchema =
        new Schema(
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        // x renamed to z
                        Types.NestedField.required(19, "z", Types.IntegerType.get())))));

    projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, nameMapping);
    Assert.assertEquals(
        "x is read as z", 1, ((List<Record>) projected.get("points")).get(0).get("z"));
  }

  @Test
  public void testInferredMapping() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("data", "data");

    Schema readSchema = writeSchema;
    // Pass null for nameMapping so that it is automatically inferred from read schema
    Record projected = writeAndReadWithoutFieldId(writeSchema, readSchema, record, null);
    Assert.assertEquals("id should be 34", record.get(0), projected.get(0));
    Assert.assertEquals("data should be \"data\"", record.get(1), projected.get(1).toString());
  }

  @Test
  @Override
  public void testAvroArrayAsLogicalMap() {
    // no-op
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListOfStructsProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.required(19, "x", Types.IntegerType.get()),
                        Types.NestedField.optional(18, "y", Types.IntegerType.get())))));

    org.apache.avro.Schema avroWriteSchema = AvroSchemaUtil.convert(writeSchema, "table");
    AvroSchemaUtil.convertToDeriveNameMapping(avroWriteSchema);
    Record record = new Record(avroWriteSchema);
    record.put("id", 34L);
    Record p1 =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("points").schema())
                    .getElementType()));
    p1.put("x", 1);
    p1.put("y", 2);
    Record p2 = new Record(p1.getSchema());
    p2.put("x", 3);
    p2.put("y", null);
    record.put("points", ImmutableList.of(p1, p2));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    assertNotProjected("Should not project points list", projected, "points");
    Record projectedWithoutFieldId =
        writeAndReadWithoutFieldId(writeSchema, idOnly, record, MappingUtil.create(writeSchema));
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projectedWithoutFieldId.get("id"));
    assertNotProjected("Should not project points list", projectedWithoutFieldId, "points");

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    assertNotProjected("Should not project id", projected, "id");
    Assert.assertEquals(
        "Should project points list", record.get("points"), projected.get("points"));
    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(
            writeSchema, writeSchema.select("points"), record, MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    List<Record> points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    Record projectedP1 = points.get(0);
    Assert.assertEquals("Should project x", 1, (int) projectedP1.get("x"));
    Assert.assertEquals("Should project y", 2, (int) projectedP1.get("y"));
    Record projectedP2 = points.get(1);
    Assert.assertEquals("Should project x", 3, (int) projectedP2.get("x"));
    Assert.assertEquals("Should project y", null, (Integer) projectedP2.get("y"));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    assertNotProjected("Should not project id", projected, "id");
    Assert.assertNotNull("Should project points list", projected.get("points"));
    points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    Assert.assertEquals("Should project x", 1, (int) projectedP1.get("x"));
    assertNotProjected("Should not project y", projectedP1, "y");
    projectedP2 = points.get(1);
    Assert.assertEquals("Should project x", 3, (int) projectedP2.get("x"));
    assertNotProjected("Should not project y", projectedP2, "y");

    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(
            writeSchema, writeSchema.select("points.x"), record, MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    Assert.assertNotNull("Should project points list", projectedWithoutFieldId.get("points"));
    points = (List<Record>) projectedWithoutFieldId.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    Assert.assertEquals("Should project x", 1, (int) projectedP1.get("x"));
    assertNotProjected("Should not project y", projectedP1, "y");
    projectedP2 = points.get(1);
    Assert.assertEquals("Should project x", 3, (int) projectedP2.get("x"));
    assertNotProjected("Should not project y", projectedP2, "y");

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    assertNotProjected("Should not project id", projected, "id");
    Assert.assertNotNull("Should project points list", projected.get("points"));
    points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    assertNotProjected("Should not project x", projectedP1, "x");
    Assert.assertEquals("Should project y", 2, (int) projectedP1.get("y"));
    projectedP2 = points.get(1);
    assertNotProjected("Should not project x", projectedP2, "x");
    Assert.assertEquals("Should project null y", null, projectedP2.get("y"));

    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(
            writeSchema, writeSchema.select("points.y"), record, MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    Assert.assertNotNull("Should project points list", projectedWithoutFieldId.get("points"));
    points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    assertNotProjected("Should not project x", projectedP1, "x");
    Assert.assertEquals("Should project y", 2, (int) projectedP1.get("y"));
    projectedP2 = points.get(1);
    assertNotProjected("Should not project x", projectedP2, "x");
    Assert.assertEquals("Should project null y", null, projectedP2.get("y"));

    Schema yRenamed =
        new Schema(
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.optional(18, "z", Types.IntegerType.get())))));

    projected = writeAndRead("y_renamed", writeSchema, yRenamed, record);
    assertNotProjected("Should not project id", projected, "id");
    Assert.assertNotNull("Should project points list", projected.get("points"));
    points = (List<Record>) projected.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    assertNotProjected("Should not project x", projectedP1, "x");
    assertNotProjected("Should not project y", projectedP1, "y");
    Assert.assertEquals("Should project z", 2, (int) projectedP1.get("z"));
    projectedP2 = points.get(1);
    assertNotProjected("Should not project x", projectedP2, "x");
    assertNotProjected("Should not project y", projectedP2, "y");
    Assert.assertEquals("Should project null z", null, projectedP2.get("z"));

    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(writeSchema, yRenamed, record, MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    Assert.assertNotNull("Should project points list", projectedWithoutFieldId.get("points"));
    points = (List<Record>) projectedWithoutFieldId.get("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    assertNotProjected("Should not project x", projectedP1, "x");
    assertNotProjected("Should not project y", projectedP1, "y");
    Assert.assertEquals("Should project z", 2, (int) projectedP1.get("z"));
    projectedP2 = points.get(1);
    assertNotProjected("Should not project x", projectedP2, "x");
    assertNotProjected("Should not project y", projectedP2, "y");
    Assert.assertEquals("Should project null z", null, projectedP2.get("z"));
  }

  @Test
  public void testMapOfStructsProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "locations",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.required(2, "long", Types.FloatType.get())))));

    org.apache.avro.Schema avroWriteSchema = AvroSchemaUtil.convert(writeSchema, "table");
    AvroSchemaUtil.convertToDeriveNameMapping(avroWriteSchema);

    Record record = new Record(avroWriteSchema);
    record.put("id", 34L);
    Record l1 =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("locations").schema())
                    .getValueType()));
    l1.put("lat", 53.992811f);
    l1.put("long", -1.542616f);
    Record l2 = new Record(l1.getSchema());
    l2.put("lat", 52.995143f);
    l2.put("long", -1.539054f);
    record.put("locations", ImmutableMap.of("L1", l1, "L2", l2));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.get("id"));
    assertNotProjected("Should not project locations map", projected, "locations");
    Record projectedWithoutFieldId =
        writeAndReadWithoutFieldId(writeSchema, idOnly, record, MappingUtil.create(writeSchema));
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projectedWithoutFieldId.get("id"));
    assertNotProjected("Should not project locations map", projectedWithoutFieldId, "locations");

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    assertNotProjected("Should not project id", projected, "id");
    Assert.assertEquals(
        "Should project locations map",
        record.get("locations"),
        toStringMap((Map) projected.get("locations")));
    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(
            writeSchema, writeSchema.select("locations"), record, MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    verifyLocationData(projectedWithoutFieldId);

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    assertNotProjected("Should not project id", projected, "id");
    verifyLatOnlyLocationData(projected);
    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(
            writeSchema,
            writeSchema.select("locations.lat"),
            record,
            MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    verifyLatOnlyLocationData(projectedWithoutFieldId);

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    assertNotProjected("Should not project id", projected, "id");
    verifyLongOnlyLocationData(projected);
    projectedWithoutFieldId =
        writeAndReadWithoutFieldId(
            writeSchema,
            writeSchema.select("locations.long"),
            record,
            MappingUtil.create(writeSchema));
    assertNotProjected("Should not project id", projectedWithoutFieldId, "id");
    verifyLongOnlyLocationData(projectedWithoutFieldId);

    Schema latitiudeRenamed =
        new Schema(
            Types.NestedField.optional(
                5,
                "locations",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "latitude", Types.FloatType.get())))));

    projected = writeAndRead("latitude_renamed", writeSchema, latitiudeRenamed, record);
    assertNotProjected("Should not project id", projected, "id");
    verifyLatRenameLocationData(projected);
  }

  private void verifyLatOnlyLocationData(Record record) {
    Map<String, ?> locations = toStringMap((Map) record.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    Record projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals(
        "L1 should contain lat", 53.992811f, (float) projectedL1.get("lat"), 0.000001);
    assertNotProjected("L1 should not contain long", projectedL1, "long");
    Record projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals(
        "L2 should contain lat", 52.995143f, (float) projectedL2.get("lat"), 0.000001);
    assertNotProjected("L1 should not contain long", projectedL1, "long");
  }

  private void verifyLongOnlyLocationData(Record record) {
    Map<String, ?> locations = toStringMap((Map) record.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    Record projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    assertNotProjected("L1 should not contain lat", projectedL1, "lat");
    Assert.assertEquals(
        "L1 should contain long", -1.542616f, (float) projectedL1.get("long"), 0.000001);
    Record projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    assertNotProjected("L2 should not contain lat", projectedL2, "lat");
    Assert.assertEquals(
        "L2 should contain long", -1.539054f, (float) projectedL2.get("long"), 0.000001);
  }

  private void verifyLatRenameLocationData(Record record) {
    Map<String, ?> locations = toStringMap((Map) record.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    Record projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals(
        "L1 should contain latitude", 53.992811f, (float) projectedL1.get("latitude"), 0.000001);
    assertNotProjected("L1 should not contain lat", projectedL1, "lat");
    assertNotProjected("L1 should not contain long", projectedL1, "long");
    Record projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals(
        "L2 should contain latitude", 52.995143f, (float) projectedL2.get("latitude"), 0.000001);
    assertNotProjected("L2 should not contain lat", projectedL2, "lat");
    assertNotProjected("L2 should not contain long", projectedL2, "long");
  }

  private void verifyLocationData(Record record) {
    Map<String, ?> locations = toStringMap((Map) record.get("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    Record projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals(
        "L1 should contain lat", 53.992811f, (float) projectedL1.get("lat"), 0.000001);
    Assert.assertEquals(
        "L1 should contain long", -1.542616f, (float) projectedL1.get("long"), 0.000001);
    Record projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals(
        "L2 should contain lat", 52.995143f, (float) projectedL2.get("lat"), 0.000001);
    Assert.assertEquals(
        "L2 should contain long", -1.539054f, (float) projectedL2.get("long"), 0.000001);
  }

  @Override
  protected Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, Record inputRecord) throws IOException {
    return super.writeAndRead(desc, writeSchema, readSchema, inputRecord);
  }

  private Record writeAndReadWithoutFieldId(
      Schema writeSchema, Schema readSchema, Record record, NameMapping nameMapping)
      throws IOException {

    File file = temp.newFile();
    // Write without file ids
    org.apache.avro.Schema writeAvroSchema = RemoveIds.removeIds(writeSchema);
    DatumWriter<Record> datumWriter = new GenericDatumWriter<>(writeAvroSchema);
    try (DataFileWriter<Record> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(writeAvroSchema, file);
      dataFileWriter.append(record);
    }

    Iterable<GenericData.Record> records =
        Avro.read(Files.localInput(file)).project(readSchema).withNameMapping(nameMapping).build();

    return Iterables.getOnlyElement(records);
  }

  private Map<String, ?> toStringMap(Map<?, ?> map) {
    Map<String, Object> stringMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getValue() instanceof CharSequence) {
        stringMap.put(entry.getKey().toString(), entry.getValue().toString());
      } else {
        stringMap.put(entry.getKey().toString(), entry.getValue());
      }
    }
    return stringMap;
  }

  private static void assertNotProjected(String message, Record projected, String fieldName) {
    try {
      projected.get(fieldName);
      Assert.fail(message);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof AvroRuntimeException);
    }
  }
}
