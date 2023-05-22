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
package org.apache.iceberg.transforms;

import static org.apache.iceberg.TestHelpers.assertAndUnwrap;
import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestProjection {
  private static final Schema SCHEMA = new Schema(optional(16, "id", Types.LongType.get()));

  @Test
  public void testIdentityProjection() {
    List<UnboundPredicate<?>> predicates =
        Lists.newArrayList(
            Expressions.notNull("id"),
            Expressions.isNull("id"),
            Expressions.lessThan("id", 100),
            Expressions.lessThanOrEqual("id", 101),
            Expressions.greaterThan("id", 102),
            Expressions.greaterThanOrEqual("id", 103),
            Expressions.equal("id", 104),
            Expressions.notEqual("id", 105));

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();

    for (UnboundPredicate<?> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.inclusive(spec).project(predicate);
      UnboundPredicate<?> projected = assertAndUnwrapUnbound(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundPredicate<?> bound = assertAndUnwrap(predicate.bind(spec.schema().asStruct(), true));

      Assert.assertEquals(
          "Field name should match partition struct field", "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      if (bound.isLiteralPredicate()) {
        Assert.assertEquals(
            "Literal should be equal",
            bound.asLiteralPredicate().literal().value(),
            projected.literal().value());
      } else {
        Assert.assertNull("Literal should be null", projected.literal());
      }
    }
  }

  @Test
  public void testCaseInsensitiveIdentityProjection() {
    List<UnboundPredicate<?>> predicates =
        Lists.newArrayList(
            Expressions.notNull("ID"),
            Expressions.isNull("ID"),
            Expressions.lessThan("ID", 100),
            Expressions.lessThanOrEqual("ID", 101),
            Expressions.greaterThan("ID", 102),
            Expressions.greaterThanOrEqual("ID", 103),
            Expressions.equal("ID", 104),
            Expressions.notEqual("ID", 105));

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();

    for (UnboundPredicate<?> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.inclusive(spec, false).project(predicate);
      UnboundPredicate<?> projected = assertAndUnwrapUnbound(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundPredicate<?> bound = assertAndUnwrap(predicate.bind(spec.schema().asStruct(), false));

      Assert.assertEquals(
          "Field name should match partition struct field", "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      if (bound.isLiteralPredicate()) {
        Assert.assertEquals(
            "Literal should be equal",
            bound.asLiteralPredicate().literal().value(),
            projected.literal().value());
      } else {
        Assert.assertNull("Literal should be null", projected.literal());
      }
    }
  }

  @Test
  public void testCaseSensitiveIdentityProjection() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Assertions.assertThatThrownBy(
            () -> Projections.inclusive(spec, true).project(Expressions.notNull("ID")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID' in struct");
  }

  @Test
  public void testStrictIdentityProjection() {
    List<UnboundPredicate<?>> predicates =
        Lists.newArrayList(
            Expressions.notNull("id"),
            Expressions.isNull("id"),
            Expressions.lessThan("id", 100),
            Expressions.lessThanOrEqual("id", 101),
            Expressions.greaterThan("id", 102),
            Expressions.greaterThanOrEqual("id", 103),
            Expressions.equal("id", 104),
            Expressions.notEqual("id", 105));

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();

    for (UnboundPredicate<?> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.strict(spec).project(predicate);
      UnboundPredicate<?> projected = assertAndUnwrapUnbound(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundPredicate<?> bound = assertAndUnwrap(predicate.bind(spec.schema().asStruct(), true));

      Assert.assertEquals(
          "Field name should match partition struct field", "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      if (bound.isLiteralPredicate()) {
        Assert.assertEquals(
            "Literal should be equal",
            bound.asLiteralPredicate().literal().value(),
            projected.literal().value());
      } else {
        Assert.assertNull("Literal should be null", projected.literal());
      }
    }
  }

  @Test
  public void testCaseInsensitiveStrictIdentityProjection() {
    List<UnboundPredicate<?>> predicates =
        Lists.newArrayList(
            Expressions.notNull("ID"),
            Expressions.isNull("ID"),
            Expressions.lessThan("ID", 100),
            Expressions.lessThanOrEqual("ID", 101),
            Expressions.greaterThan("ID", 102),
            Expressions.greaterThanOrEqual("ID", 103),
            Expressions.equal("ID", 104),
            Expressions.notEqual("ID", 105));

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();

    for (UnboundPredicate<?> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.strict(spec, false).project(predicate);
      UnboundPredicate<?> projected = assertAndUnwrapUnbound(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundPredicate<?> bound = assertAndUnwrap(predicate.bind(spec.schema().asStruct(), false));

      Assert.assertEquals(
          "Field name should match partition struct field", "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      if (bound.isLiteralPredicate()) {
        Assert.assertEquals(
            "Literal should be equal",
            bound.asLiteralPredicate().literal().value(),
            projected.literal().value());
      } else {
        Assert.assertNull("Literal should be null", projected.literal());
      }
    }
  }

  @Test
  public void testCaseSensitiveStrictIdentityProjection() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Assertions.assertThatThrownBy(
            () -> Projections.strict(spec, true).project(Expressions.notNull("ID")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID' in struct");
  }

  @Test
  public void testBadSparkPartitionFilter() {
    // this tests a case that results in a full table scan in Spark with Hive tables. because the
    // hour field is not a partition, mixing it with partition columns in the filter expression
    // prevents the day/hour boundaries from being pushed to the metastore. this is an easy mistake
    // when tables are normally partitioned by both hour and dateint. the the filter is:
    //
    // WHERE dateint = 20180416
    //   OR (dateint = 20180415 and hour >= 20)
    //   OR (dateint = 20180417 and hour <= 4)

    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            required(3, "hour", Types.IntegerType.get()),
            required(4, "dateint", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dateint").build();

    Expression filter =
        or(
            equal("dateint", 20180416),
            or(
                and(equal("dateint", 20180415), greaterThanOrEqual("hour", 20)),
                and(equal("dateint", 20180417), lessThanOrEqual("hour", 4))));

    Expression projection = Projections.inclusive(spec).project(filter);

    Assertions.assertThat(projection).isInstanceOf(Or.class);
    Or or1 = (Or) projection;
    UnboundPredicate<?> dateint1 = assertAndUnwrapUnbound(or1.left());
    Assert.assertEquals("Should be a dateint predicate", "dateint", dateint1.ref().name());
    Assert.assertEquals("Should be dateint=20180416", 20180416, dateint1.literal().value());
    Assertions.assertThat(or1.right()).isInstanceOf(Or.class);
    Or or2 = (Or) or1.right();
    UnboundPredicate<?> dateint2 = assertAndUnwrapUnbound(or2.left());
    Assert.assertEquals("Should be a dateint predicate", "dateint", dateint2.ref().name());
    Assert.assertEquals("Should be dateint=20180415", 20180415, dateint2.literal().value());
    UnboundPredicate<?> dateint3 = assertAndUnwrapUnbound(or2.right());
    Assert.assertEquals("Should be a dateint predicate", "dateint", dateint3.ref().name());
    Assert.assertEquals("Should be dateint=20180417", 20180417, dateint3.literal().value());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProjectionNames() {
    final Schema schema =
        new Schema(
            required(1, "timestamp1", Types.TimestampType.withoutZone()),
            optional(2, "timestamp2", Types.TimestampType.withoutZone()),
            optional(3, "timestamp3", Types.TimestampType.withoutZone()),
            optional(4, "timestamp4", Types.TimestampType.withoutZone()),
            optional(5, "date1", Types.DateType.get()),
            optional(6, "date2", Types.DateType.get()),
            optional(7, "date3", Types.DateType.get()),
            optional(8, "long", Types.LongType.get()),
            optional(9, "string", Types.StringType.get()));

    final PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema)
            .withSpecId(0)
            .hour("timestamp1")
            .day("timestamp2")
            .month("timestamp3")
            .year("timestamp4")
            .day("date1")
            .month("date2")
            .year("date3")
            .bucket("long", 10)
            .truncate("string", 10)
            .build();

    UnboundPredicate<Integer> predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(hour("timestamp1"), 20));
    Assert.assertEquals(
        "should expected timestamp1_hour", "timestamp1_hour", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(hour("timestamp1"), 20));
    Assert.assertEquals(
        "should expected timestamp1_hour", "timestamp1_hour", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(day("timestamp2"), 20));
    Assert.assertEquals("should expected timestamp2_day", "timestamp2_day", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(day("timestamp2"), 20));
    Assert.assertEquals("should expected timestamp2_day", "timestamp2_day", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(month("timestamp3"), 20));
    Assert.assertEquals(
        "should expected timestamp3_month", "timestamp3_month", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(month("timestamp3"), 20));
    Assert.assertEquals(
        "should expected timestamp3_month", "timestamp3_month", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(year("timestamp4"), 20));
    Assert.assertEquals(
        "should expected timestamp4_year", "timestamp4_year", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(year("timestamp4"), 20));
    Assert.assertEquals(
        "should expected timestamp4_year", "timestamp4_year", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(day("date1"), 20));
    Assert.assertEquals("should expected date1_day", "date1_day", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(day("date1"), 20));
    Assert.assertEquals("should expected date1_day", "date1_day", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(month("date2"), 20));
    Assert.assertEquals("should expected date2_month", "date2_month", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(month("date2"), 20));
    Assert.assertEquals("should expected date2_month", "date2_month", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(year("date3"), 20));
    Assert.assertEquals("should expected date3_year", "date3_year", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(year("date3"), 20));
    Assert.assertEquals("should expected date3_year", "date3_year", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(bucket("long", 10), 20));
    Assert.assertEquals("should expected long_bucket", "long_bucket", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(bucket("long", 10), 20));
    Assert.assertEquals("should expected long_bucket", "long_bucket", predicate.ref().name());

    predicate =
        (UnboundPredicate<Integer>)
            Projections.strict(partitionSpec).project(equal(truncate("string", 10), "abc"));
    Assert.assertEquals("should expected string_trunc", "string_trunc", predicate.ref().name());
    predicate =
        (UnboundPredicate<Integer>)
            Projections.inclusive(partitionSpec).project(equal(truncate("string", 10), "abc"));
    Assert.assertEquals("should expected string_trunc", "string_trunc", predicate.ref().name());
  }
}
