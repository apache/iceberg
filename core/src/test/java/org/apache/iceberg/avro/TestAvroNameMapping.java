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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroNameMapping extends TestAvroReadProjection {
  @Override
  protected GenericData.Record writeAndRead(String desc,
                                            Schema writeSchema,
                                            Schema readSchema,
                                            GenericData.Record inputRecord) throws IOException {

    // Use all existing TestAvroReadProjection tests to verify that
    // we get the same projected (Avro) read schema whether we use
    // NameMapping together with file schema without field-ids or we
    // use a file schema having field-ids
    GenericData.Record record = super.writeAndRead(desc, writeSchema, readSchema, inputRecord);
    org.apache.avro.Schema expected = record.getSchema();
    org.apache.avro.Schema actual = projectWithNameMapping(writeSchema, readSchema, MappingUtil.create(writeSchema));
    Assert.assertEquals(expected, actual);
    return record;
  }

  @Test
  public void testNameMappingProjections() {
    Schema fileSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get()),
                Types.NestedField.required(2, "long", Types.FloatType.get())
            )
        )));

    // Table mapping does not project `locations` map
    NameMapping nameMapping = MappingUtil.create(new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())));

    // Table read schema projects `locations` map's value
    Schema readSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(2, "long", Types.FloatType.get())
            )
        )));

    check(fileSchema, readSchema, nameMapping);
  }

  @Test
  public void testMissingFields() {
    Schema fileSchema = new Schema(
        Types.NestedField.required(19, "x", Types.IntegerType.get()),
        Types.NestedField.optional(18, "y", Types.IntegerType.get()));

    // table mapping not projecting a required field 'x'
    NameMapping nameMapping = MappingUtil.create(new Schema(
        Types.NestedField.optional(18, "y", Types.IntegerType.get())));

    Schema readSchema = fileSchema;
    AssertHelpers.assertThrows("Missing required field in nameMapping",
        IllegalArgumentException.class, "Missing required field: x",
        // In this case, pruneColumns result is an empty record
        () -> projectWithNameMapping(fileSchema, readSchema, nameMapping));
  }

  @Test
  public void testComplexMapKeys() {
    Schema fileSchema = new Schema(
        Types.NestedField.required(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StructType.of(
                Types.NestedField.required(3, "k1", Types.StringType.get()),
                Types.NestedField.optional(4, "k2", Types.StringType.get())
            ),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get()),
                Types.NestedField.optional(2, "long", Types.FloatType.get())
            )
        )));

    // project a subset of the map's value columns in NameMapping
    NameMapping nameMapping = MappingUtil.create(new Schema(
        Types.NestedField.required(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StructType.of(
                Types.NestedField.required(3, "k1", Types.StringType.get()),
                Types.NestedField.optional(4, "k2", Types.StringType.get())
            ),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get())
            )
        ))));

    Schema readSchema = new Schema(
        Types.NestedField.required(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StructType.of(
                Types.NestedField.required(3, "k1", Types.StringType.get()),
                Types.NestedField.optional(4, "k2", Types.StringType.get())
            ),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get()),
                Types.NestedField.optional(2, "long", Types.FloatType.get())
            )
        )));

    check(fileSchema, readSchema, nameMapping);
  }

  @Test
  public void testArrayProjections() {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.required(19, "x", Types.IntegerType.get()),
                Types.NestedField.optional(18, "y", Types.IntegerType.get())
            ))
        )
    );

    NameMapping nameMapping = MappingUtil.create(new Schema(
        // Optional array field missing. Will be filled in
        // using default values using read schema
        Types.NestedField.required(0, "id", Types.LongType.get())));

    Schema readSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.required(19, "x", Types.IntegerType.get())
            ))
        )
    );

    check(writeSchema, readSchema, nameMapping);

    // points array is partially projected
    nameMapping = MappingUtil.create(new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.required(19, "x", Types.IntegerType.get())))
        )
    ));

    check(writeSchema, readSchema, nameMapping);
  }

  @Test
  public void testAliases() {
    Schema fileSchema = new Schema(
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.required(19, "x", Types.IntegerType.get())
            ))
        )
    );

    NameMapping nameMapping = NameMapping.of(MappedFields.of(
        MappedField.of(22, "points", MappedFields.of(
            MappedField.of(19, Lists.newArrayList("x", "y", "z"))
        ))));


    Schema readSchema = new Schema(
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                // x renamed to y
                Types.NestedField.required(19, "y", Types.IntegerType.get())
            ))
        )
    );

    check(fileSchema, readSchema, nameMapping);

    readSchema = new Schema(
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                // x renamed to z
                Types.NestedField.required(19, "z", Types.IntegerType.get())
            ))
        )
    );

    check(fileSchema, readSchema, nameMapping);
  }


  private static org.apache.avro.Schema project(Schema writeSchema, Schema readSchema) {
    // Build a read schema when file schema has field ids
    org.apache.avro.Schema avroFileSchema = AvroSchemaUtil.convert(writeSchema.asStruct(), "table");
    Set<Integer> projectedIds = TypeUtil.getProjectedIds(readSchema);
    org.apache.avro.Schema prunedFileSchema = AvroSchemaUtil.pruneColumns(avroFileSchema, projectedIds, null);
    return AvroSchemaUtil.buildAvroProjection(prunedFileSchema, readSchema, Collections.emptyMap());
  }

  private static org.apache.avro.Schema projectWithNameMapping(
      Schema writeSchema, Schema readSchema, NameMapping nameMapping) {
    // Build a read schema when file schema does not have field ids . The field ids are provided by `nameMapping`
    org.apache.avro.Schema avroFileSchema = removeIds(writeSchema);
    Set<Integer> projectedIds = TypeUtil.getProjectedIds(readSchema);
    org.apache.avro.Schema prunedFileSchema = AvroSchemaUtil.pruneColumns(avroFileSchema, projectedIds, nameMapping);
    return AvroSchemaUtil.buildAvroProjection(prunedFileSchema, readSchema, Collections.emptyMap());
  }

  private void check(Schema writeSchema, Schema readSchema, NameMapping nameMapping) {
    org.apache.avro.Schema expected = project(writeSchema, readSchema);
    org.apache.avro.Schema actual = projectWithNameMapping(writeSchema, readSchema, nameMapping);

    // `BuildAvroProjection` can skip adding fields ids in some cases.
    // e.g when creating a map if the value is modified. This leads to
    // test failures. For now I've removed ids to perform equality testing.
    Assert.assertEquals(
        "Read schema built using external mapping should match read schema built with file schema having field ids",
        SchemaNormalization.parsingFingerprint64(expected),
        SchemaNormalization.parsingFingerprint64(actual));
    Assert.assertEquals(
        "Projected/read schema built using external mapping will always match expected Iceberg schema",
        SchemaNormalization.parsingFingerprint64(AvroSchemaUtil.convert(readSchema, "table")),
        SchemaNormalization.parsingFingerprint64(actual));
  }

  private static org.apache.avro.Schema removeIds(Schema schema) {
    return AvroSchemaVisitor.visit(AvroSchemaUtil.convert(schema.asStruct(), "table"), new RemoveIds());
  }
}
