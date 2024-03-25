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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SortOrderUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSortOrder {

  // column ids will be reassigned during table creation
  private static final Schema SCHEMA =
      new Schema(
          required(10, "id", Types.IntegerType.get()),
          required(11, "data", Types.StringType.get()),
          required(40, "d", Types.DateType.get()),
          required(41, "ts", Types.TimestampType.withZone()),
          optional(
              12,
              "s",
              Types.StructType.of(
                  required(17, "id", Types.IntegerType.get()),
                  optional(
                      18,
                      "b",
                      Types.ListType.ofOptional(
                          3,
                          Types.StructType.of(
                              optional(19, "i", Types.IntegerType.get()),
                              optional(20, "s", Types.StringType.get())))))),
          required(30, "ext", Types.StringType.get()),
          required(42, "Ext1", Types.StringType.get()));

  @TempDir private Path temp;

  private File tableDir = null;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @Parameter private int formatVersion;

  @BeforeEach
  public void setupTableDir() throws IOException {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void testSortOrderBuilder() {
    assertThat(SortOrder.builderFor(SCHEMA).withOrderId(0).build()).isEqualTo(SortOrder.unsorted());

    assertThatThrownBy(() -> SortOrder.builderFor(SCHEMA).asc("data").withOrderId(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Sort order ID 0 is reserved for unsorted order");

    assertThatThrownBy(() -> SortOrder.builderFor(SCHEMA).withOrderId(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsorted order ID must be 0");
  }

  @TestTemplate
  public void testDefaultOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, formatVersion);
    assertThat(table.sortOrders()).hasSize(1);

    SortOrder actualOrder = table.sortOrder();
    assertThat(actualOrder.orderId()).isEqualTo(0);
    assertThat(actualOrder.isUnsorted()).isTrue();
  }

  @TestTemplate
  public void testFreshIds() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).withSpecId(5).identity("data").build();
    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(10)
            .asc("s.id", NULLS_LAST)
            .desc(truncate("data", 10), NULLS_FIRST)
            .build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    assertThat(table.sortOrders()).hasSize(1).containsKey(TableMetadata.INITIAL_SORT_ORDER_ID);

    SortOrder actualOrder = table.sortOrder();
    assertThat(actualOrder.orderId()).isEqualTo(TableMetadata.INITIAL_SORT_ORDER_ID);
    assertThat(actualOrder.fields()).hasSize(2);
    assertThat(actualOrder.fields().get(0).sourceId()).isEqualTo(8);
    assertThat(actualOrder.fields().get(1).sourceId()).isEqualTo(2);
  }

  @TestTemplate
  public void testCompatibleOrders() {
    SortOrder order1 = SortOrder.builderFor(SCHEMA).withOrderId(9).asc("s.id", NULLS_LAST).build();

    SortOrder order2 =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(10)
            .asc("s.id", NULLS_LAST)
            .desc(truncate("data", 10), NULLS_FIRST)
            .build();

    SortOrder order3 =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(11)
            .asc("s.id", NULLS_LAST)
            .desc(truncate("data", 10), NULLS_LAST)
            .build();

    SortOrder order4 =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(11)
            .asc("s.id", NULLS_LAST)
            .asc(truncate("data", 10), NULLS_FIRST)
            .build();

    SortOrder order5 =
        SortOrder.builderFor(SCHEMA).withOrderId(11).desc("s.id", NULLS_LAST).build();

    // an unsorted order satisfies only itself
    assertThat(SortOrder.unsorted().satisfies(SortOrder.unsorted())).isTrue();
    assertThat(SortOrder.unsorted().satisfies(order1)).isFalse();
    assertThat(SortOrder.unsorted().satisfies(order2)).isFalse();
    assertThat(SortOrder.unsorted().satisfies(order3)).isFalse();
    assertThat(SortOrder.unsorted().satisfies(order4)).isFalse();
    assertThat(SortOrder.unsorted().satisfies(order5)).isFalse();

    // any ordering satisfies an unsorted ordering
    assertThat(order1.satisfies(SortOrder.unsorted())).isTrue();
    assertThat(order2.satisfies(SortOrder.unsorted())).isTrue();
    assertThat(order3.satisfies(SortOrder.unsorted())).isTrue();
    assertThat(order4.satisfies(SortOrder.unsorted())).isTrue();
    assertThat(order5.satisfies(SortOrder.unsorted())).isTrue();

    // order1 has the same fields but different sort direction compared to order5
    assertThat(order1.satisfies(order5)).isFalse();

    // order2 has more fields than order1 and is compatible
    assertThat(order2.satisfies(order1)).isTrue();
    // order2 has more fields than order5 but is incompatible
    assertThat(order2.satisfies(order5)).isFalse();
    // order2 has the same fields but different null order compared to order3
    assertThat(order2.satisfies(order3)).isFalse();
    // order2 has the same fields but different sort direction compared to order4
    assertThat(order2.satisfies(order4)).isFalse();

    // order1 has fewer fields than order2 and is incompatible
    assertThat(order1.satisfies(order2)).isFalse();
  }

  @TestTemplate
  public void testSatisfiesTruncateFieldOrder() {
    SortOrder id = SortOrder.builderFor(SCHEMA).asc("data", NULLS_LAST).build();
    SortOrder truncate4 =
        SortOrder.builderFor(SCHEMA).asc(Expressions.truncate("data", 4), NULLS_LAST).build();
    SortOrder truncate2 =
        SortOrder.builderFor(SCHEMA).asc(Expressions.truncate("data", 2), NULLS_LAST).build();

    assertThat(id.satisfies(truncate2)).isTrue();
    assertThat(truncate2.satisfies(id)).isFalse();
    assertThat(truncate4.satisfies(id)).isFalse();
    assertThat(truncate4.satisfies(truncate2)).isTrue();
    assertThat(truncate2.satisfies(truncate4)).isFalse();
  }

  @TestTemplate
  public void testSatisfiesDateFieldOrder() {
    SortOrder id = SortOrder.builderFor(SCHEMA).asc("d", NULLS_LAST).build();
    SortOrder year = SortOrder.builderFor(SCHEMA).asc(Expressions.year("d"), NULLS_LAST).build();
    SortOrder month = SortOrder.builderFor(SCHEMA).asc(Expressions.month("d"), NULLS_LAST).build();
    SortOrder day = SortOrder.builderFor(SCHEMA).asc(Expressions.day("d"), NULLS_LAST).build();

    assertThat(id.satisfies(year)).isTrue();
    assertThat(id.satisfies(month)).isTrue();
    assertThat(id.satisfies(day)).isTrue();
    assertThat(year.satisfies(id)).isFalse();
    assertThat(month.satisfies(id)).isFalse();
    assertThat(day.satisfies(id)).isFalse();
    assertThat(day.satisfies(year)).isTrue();
    assertThat(day.satisfies(month)).isTrue();
    assertThat(month.satisfies(year)).isTrue();
    assertThat(month.satisfies(day)).isFalse();
    assertThat(year.satisfies(day)).isFalse();
    assertThat(year.satisfies(month)).isFalse();
  }

  @TestTemplate
  public void testSatisfiesTimestampFieldOrder() {
    SortOrder id = SortOrder.builderFor(SCHEMA).asc("ts", NULLS_LAST).build();
    SortOrder year = SortOrder.builderFor(SCHEMA).asc(Expressions.year("ts"), NULLS_LAST).build();
    SortOrder month = SortOrder.builderFor(SCHEMA).asc(Expressions.month("ts"), NULLS_LAST).build();
    SortOrder day = SortOrder.builderFor(SCHEMA).asc(Expressions.day("ts"), NULLS_LAST).build();
    SortOrder hour = SortOrder.builderFor(SCHEMA).asc(Expressions.hour("ts"), NULLS_LAST).build();

    assertThat(id.satisfies(year)).isTrue();
    assertThat(id.satisfies(month)).isTrue();
    assertThat(id.satisfies(day)).isTrue();
    assertThat(id.satisfies(hour)).isTrue();
    assertThat(year.satisfies(id)).isFalse();
    assertThat(month.satisfies(id)).isFalse();
    assertThat(day.satisfies(id)).isFalse();
    assertThat(hour.satisfies(id)).isFalse();
    assertThat(hour.satisfies(year)).isTrue();
    assertThat(hour.satisfies(month)).isTrue();
    assertThat(hour.satisfies(day)).isTrue();
    assertThat(day.satisfies(year)).isTrue();
    assertThat(day.satisfies(month)).isTrue();
    assertThat(day.satisfies(hour)).isFalse();
    assertThat(month.satisfies(year)).isTrue();
    assertThat(month.satisfies(day)).isFalse();
    assertThat(month.satisfies(hour)).isFalse();
    assertThat(year.satisfies(day)).isFalse();
    assertThat(year.satisfies(month)).isFalse();
    assertThat(year.satisfies(hour)).isFalse();
  }

  @TestTemplate
  public void testSameOrder() {
    SortOrder order1 = SortOrder.builderFor(SCHEMA).withOrderId(9).asc("s.id", NULLS_LAST).build();

    SortOrder order2 = SortOrder.builderFor(SCHEMA).withOrderId(10).asc("s.id", NULLS_LAST).build();

    // orders have different ids but are logically the same
    assertThat(order2).isNotEqualTo(order1);
    assertThat(order2.fields()).isEqualTo(order1.fields());
  }

  @TestTemplate
  public void testSchemaEvolutionWithSortOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order =
        SortOrder.builderFor(SCHEMA).withOrderId(10).asc("s.id").desc(truncate("data", 10)).build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    table.updateSchema().renameColumn("s.id", "s.id2").commit();

    SortOrder actualOrder = table.sortOrder();
    assertThat(actualOrder.orderId()).isEqualTo(TableMetadata.INITIAL_SORT_ORDER_ID);
    assertThat(actualOrder.fields()).hasSize(2);
    assertThat(actualOrder.fields().get(0).sourceId()).isEqualTo(8);
    assertThat(actualOrder.fields().get(1).sourceId()).isEqualTo(2);
  }

  @TestTemplate
  public void testColumnDropWithSortOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();

    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, formatVersion);

    int initialColSize = table.schema().columns().size();

    table.replaceSortOrder().asc("id").commit();
    table.replaceSortOrder().asc("data").commit();

    table.updateSchema().deleteColumn("id").commit();

    SortOrder actualOrder = table.sortOrder();
    assertThat(actualOrder.orderId()).isEqualTo(TableMetadata.INITIAL_SORT_ORDER_ID + 1);
    assertThat(table.schema().columns()).hasSize(initialColSize - 1);

    // ensure that the table metadata can be serialized and reloaded with an invalid order
    TableMetadataParser.fromJson(TableMetadataParser.toJson(table.ops().current()));
  }

  @TestTemplate
  public void testIncompatibleSchemaEvolutionWithSortOrder() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    SortOrder order =
        SortOrder.builderFor(SCHEMA).withOrderId(10).asc("s.id").desc(truncate("data", 10)).build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, order, formatVersion);

    assertThatThrownBy(() -> table.updateSchema().deleteColumn("s.id").commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find source column for sort field");
  }

  @TestTemplate
  public void testEmptySortOrder() {
    SortOrder order = SortOrder.builderFor(SCHEMA).build();
    assertThat(order).isEqualTo(SortOrder.unsorted());
  }

  @TestTemplate
  public void testSortedColumnNames() {
    SortOrder order =
        SortOrder.builderFor(SCHEMA).withOrderId(10).asc("s.id").desc(truncate("data", 10)).build();
    Set<String> sortedCols = SortOrderUtil.orderPreservingSortedColumns(order);
    assertThat(sortedCols).containsExactly("s.id", "data");
  }

  @TestTemplate
  public void testPreservingOrderSortedColumnNames() {
    SortOrder order =
        SortOrder.builderFor(SCHEMA)
            .withOrderId(10)
            .asc(bucket("s.id", 5))
            .desc(truncate("data", 10))
            .build();
    Set<String> sortedCols = SortOrderUtil.orderPreservingSortedColumns(order);
    assertThat(sortedCols).containsExactly("data");
  }

  @TestTemplate
  public void testCaseSensitiveSortedColumnNames() {
    String fieldName = "ext1";
    assertThatThrownBy(
            () ->
                SortOrder.builderFor(SCHEMA)
                    .caseSensitive(true)
                    .withOrderId(10)
                    .asc(fieldName)
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(String.format("Cannot find field '%s' in struct", fieldName));

    SortOrder ext1 =
        SortOrder.builderFor(SCHEMA).caseSensitive(false).withOrderId(10).asc("ext1").build();
    SortField sortField = ext1.fields().get(0);
    assertThat(SCHEMA.findField("Ext1").fieldId()).isEqualTo(sortField.sourceId());
  }
}
