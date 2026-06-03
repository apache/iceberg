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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkTableUuidMetadataDelete extends CatalogTestBase {
  private static final String LEGACY_SIGNED_UUID_FILE = "/path/to/data/legacy-signed-uuid.parquet";
  private static final String UNSIGNED_UUID_FILE = "/path/to/data/unsigned-uuid.parquet";
  private static final String ID_BOUNDS_FILE = "/path/to/data/id-bounds.parquet";

  private static final int ID_FIELD_ID = 1;
  private static final int UUID_FIELD_ID = 2;

  private static final UUID UUID_00 = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final UUID UUID_40 = UUID.fromString("40000000-0000-0000-0000-000000000001");
  private static final UUID UUID_7F = UUID.fromString("7fffffff-ffff-ffff-ffff-ffffffffffff");
  private static final UUID UUID_80 = UUID.fromString("80000000-0000-0000-0000-000000000001");
  private static final UUID UUID_FF = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

  private static final Schema UUID_SCHEMA =
      new Schema(
          required(ID_FIELD_ID, "id", Types.IntegerType.get()),
          required(UUID_FIELD_ID, "uuid_col", Types.UUIDType.get()));

  private static final PartitionSpec UUID_SPEC = PartitionSpec.unpartitioned();
  private static final PartitionSpec UUID_PARTITION_SPEC =
      PartitionSpec.builderFor(UUID_SCHEMA).identity("uuid_col").build();

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void uuidPredicateWithLegacySignedBoundsCannotUseMetadataDelete() throws Exception {
    Table table =
        createTableWithFile(
            dataFile(
                LEGACY_SIGNED_UUID_FILE, UUID_FIELD_ID, Types.UUIDType.get(), UUID_80, UUID_40));

    SparkTable sparkTable = sparkTable(table);
    Predicate uuidPredicate = predicate("<=", "uuid_col", uuidLiteral(UUID_40));

    assertThat(sparkTable.canDeleteWhere(new Predicate[] {uuidPredicate}))
        .as("Legacy signed UUID bounds must not be trusted for metadata delete")
        .isFalse();
  }

  @TestTemplate
  public void uuidPredicateWithUnsignedBoundsCannotUseMetadataDelete() throws Exception {
    Table table =
        createTableWithFile(
            dataFile(UNSIGNED_UUID_FILE, UUID_FIELD_ID, Types.UUIDType.get(), UUID_00, UUID_FF));

    SparkTable sparkTable = sparkTable(table);
    Predicate uuidPredicate = predicate("<=", "uuid_col", uuidLiteral(UUID_7F));

    assertThat(sparkTable.canDeleteWhere(new Predicate[] {uuidPredicate}))
        .as("UUID bounds written with the current comparator are still conservative for deletes")
        .isFalse();
  }

  @TestTemplate
  public void uuidPartitionPredicateCannotUseMetadataDelete() throws Exception {
    Table table = validationCatalog.createTable(tableIdent, UUID_SCHEMA, UUID_PARTITION_SPEC);

    SparkTable sparkTable = sparkTable(table);
    Predicate uuidPredicate = predicate("=", "uuid_col", uuidLiteral(UUID_40));

    assertThat(sparkTable.canDeleteWhere(new Predicate[] {uuidPredicate}))
        .as("UUID predicates should not use partition-only metadata deletes")
        .isFalse();
  }

  @TestTemplate
  public void nonUuidPredicateCanStillUseMetadataDelete() throws Exception {
    Table table =
        createTableWithFile(dataFile(ID_BOUNDS_FILE, ID_FIELD_ID, Types.IntegerType.get(), 1, 4));

    SparkTable sparkTable = sparkTable(table);
    Predicate idPredicate = predicate("<=", "id", new LiteralValue<>(4, DataTypes.IntegerType));

    assertThat(sparkTable.canDeleteWhere(new Predicate[] {idPredicate}))
        .as("Non-UUID metadata delete should keep using strict metrics")
        .isTrue();
  }

  private Table createTableWithFile(DataFile file) {
    Table table = validationCatalog.createTable(tableIdent, UUID_SCHEMA, UUID_SPEC);
    table.newFastAppend().appendFile(file).commit();
    table.refresh();
    return table;
  }

  private static SparkTable sparkTable(Table table)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    try {
      Constructor<SparkTable> constructor = SparkTable.class.getConstructor(Table.class);
      return constructor.newInstance(table);
    } catch (NoSuchMethodException e) {
      Constructor<SparkTable> constructor =
          SparkTable.class.getConstructor(Table.class, boolean.class);
      return constructor.newInstance(table, false);
    }
  }

  private static <T> DataFile dataFile(
      String path, int fieldId, Type.PrimitiveType type, T lower, T upper) {
    return DataFiles.builder(UUID_SPEC)
        .withPath(path)
        .withFileSizeInBytes(10)
        .withMetrics(
            new Metrics(
                4L,
                null,
                ImmutableMap.of(fieldId, 4L),
                ImmutableMap.of(fieldId, 0L),
                null,
                ImmutableMap.of(fieldId, toBuffer(type, lower)),
                ImmutableMap.of(fieldId, toBuffer(type, upper))))
        .build();
  }

  private static <T> ByteBuffer toBuffer(Type.PrimitiveType type, T value) {
    return Conversions.toByteBuffer(type, value);
  }

  private static LiteralValue uuidLiteral(UUID uuid) {
    return new LiteralValue<>(UTF8String.fromString(uuid.toString()), DataTypes.StringType);
  }

  private static Predicate predicate(
      String name, String fieldName, org.apache.spark.sql.connector.expressions.Expression value) {
    org.apache.spark.sql.connector.expressions.Expression[] children =
        new org.apache.spark.sql.connector.expressions.Expression[] {
          FieldReference.apply(fieldName), value
        };
    return new Predicate(name, children);
  }
}
