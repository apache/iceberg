package org.apache.iceberg.transforms;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.*;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestTimestampsProjection {
  private static final Types.TimestampType TYPE = Types.TimestampType.withoutZone();
  private static final Schema SCHEMA = new Schema(
    optional(1, "timestamp", TYPE));

  public void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp, String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Literal literal = predicate.literal();
    Timestamps transform = (Timestamps) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((int) literal.value());
    Assert.assertEquals(output, expectedLiteral);

  }

  public void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp, String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Literal literal = predicate.literal();
    Timestamps transform = (Timestamps) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((int) literal.value());
    Assert.assertEquals(output, expectedLiteral);

  }

  @Test
  public void testMonthStrict() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12");

  }

  @Test
  public void testMonthInclusive() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12");

  }

  @Test
  public void testDayStrict() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-11-30");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-11-30");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-02");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-02");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01");

  }

  @Test
  public void testDayInclusive() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01");

  }

  @Test
  public void testYearStrict() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017");

  }

  @Test
  public void testYearInclusive() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017");

  }

  @Test
  public void testHourStrict() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionStrict(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-09");
    assertProjectionStrict(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-09");
    assertProjectionStrict(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-11");
    assertProjectionStrict(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-11");
    assertProjectionStrict(spec, notEqual("timestamp", date), Expression.Operation.NOT_EQ, "2017-12-01-10");

  }

  @Test
  public void testHourInclusive() {
    Long date = (long) Literal.of("2017-12-01T10:12:55.038194").to(TYPE).value();

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("timestamp").build();

    assertProjectionInclusive(spec, lessThan("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, lessThanOrEqual("timestamp", date), Expression.Operation.LT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, greaterThan("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, greaterThanOrEqual("timestamp", date), Expression.Operation.GT_EQ, "2017-12-01-10");
    assertProjectionInclusive(spec, equal("timestamp", date), Expression.Operation.EQ, "2017-12-01-10");

  }


}
