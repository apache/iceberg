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
package org.apache.iceberg.mapping;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMappingUpdates extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testAddColumnMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    assertThat(mapping.asMappedFields())
        .isEqualTo(MappedFields.of(MappedField.of(1, "id"), MappedField.of(2, "data")));

    table.updateSchema().addColumn("ts", Types.TimestampType.withZone()).commit();

    NameMapping updated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(updated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"), MappedField.of(2, "data"), MappedField.of(3, "ts")));
  }

  @TestTemplate
  public void testAddNestedColumnMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    assertThat(mapping.asMappedFields())
        .isEqualTo(MappedFields.of(MappedField.of(1, "id"), MappedField.of(2, "data")));

    table
        .updateSchema()
        .addColumn(
            "point",
            Types.StructType.of(
                required(1, "x", Types.DoubleType.get()), required(2, "y", Types.DoubleType.get())))
        .commit();

    NameMapping updated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(updated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"),
                MappedField.of(2, "data"),
                MappedField.of(
                    3, "point", MappedFields.of(MappedField.of(4, "x"), MappedField.of(5, "y")))));

    table.updateSchema().addColumn("point", "z", Types.DoubleType.get()).commit();

    NameMapping pointUpdated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(pointUpdated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"),
                MappedField.of(2, "data"),
                MappedField.of(
                    3,
                    "point",
                    MappedFields.of(
                        MappedField.of(4, "x"), MappedField.of(5, "y"), MappedField.of(6, "z")))));
  }

  @TestTemplate
  public void testRenameMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    assertThat(mapping.asMappedFields())
        .isEqualTo(MappedFields.of(MappedField.of(1, "id"), MappedField.of(2, "data")));

    table.updateSchema().renameColumn("id", "object_id").commit();

    NameMapping updated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(updated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, ImmutableList.of("id", "object_id")), MappedField.of(2, "data")));
  }

  @TestTemplate
  public void testRenameNestedFieldMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    table
        .updateSchema()
        .addColumn(
            "point",
            Types.StructType.of(
                required(1, "x", Types.DoubleType.get()), required(2, "y", Types.DoubleType.get())))
        .commit();

    NameMapping updated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(updated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"),
                MappedField.of(2, "data"),
                MappedField.of(
                    3, "point", MappedFields.of(MappedField.of(4, "x"), MappedField.of(5, "y")))));

    table.updateSchema().renameColumn("point.x", "X").renameColumn("point.y", "Y").commit();

    NameMapping pointUpdated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(pointUpdated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"),
                MappedField.of(2, "data"),
                MappedField.of(
                    3,
                    "point",
                    MappedFields.of(
                        MappedField.of(4, ImmutableList.of("x", "X")),
                        MappedField.of(5, ImmutableList.of("y", "Y"))))));
  }

  @TestTemplate
  public void testRenameComplexFieldMappingUpdate() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    table
        .updateSchema()
        .addColumn(
            "point",
            Types.StructType.of(
                required(1, "x", Types.DoubleType.get()), required(2, "y", Types.DoubleType.get())))
        .commit();

    NameMapping updated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(updated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"),
                MappedField.of(2, "data"),
                MappedField.of(
                    3, "point", MappedFields.of(MappedField.of(4, "x"), MappedField.of(5, "y")))));

    table.updateSchema().renameColumn("point", "p2").commit();

    NameMapping pointUpdated =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));

    assertThat(pointUpdated.asMappedFields())
        .isEqualTo(
            MappedFields.of(
                MappedField.of(1, "id"),
                MappedField.of(2, "data"),
                MappedField.of(
                    3,
                    ImmutableList.of("point", "p2"),
                    MappedFields.of(MappedField.of(4, "x"), MappedField.of(5, "y")))));
  }
}
