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

public class TestDatesProjection {
  private static final Types.DateType TYPE = Types.DateType.get();
  private static final Schema SCHEMA = new Schema(
    optional(1, "date", TYPE));

  public void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp, String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Literal literal = predicate.literal();
    Dates transform = (Dates) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((int) literal.value());
    Assert.assertEquals(output, expectedLiteral);

  }

  public void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp, String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Literal literal = predicate.literal();
    Dates transform = (Dates) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((int) literal.value());
    Assert.assertEquals(output, expectedLiteral);

  }

  @Test
  public void testMonthStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016-12");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2016-12");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-02");
    // bound should include the same month for lower bound
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-01");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-01");

  }

  @Test
  public void testMonthStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-11");
    // bound should include the same month for upper bound
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-12");

  }

  @Test
  public void testMonthInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-12-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-11");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12");

  }

  @Test
  public void testMonthInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018-01");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12");

  }

  @Test
  public void testDayStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016-12-31");
    // should be the same date for <=
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-01-01");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-01-02");
    // should be the same date for >=
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-01-01");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-01-01");

  }

  @Test
  public void testDayStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-12-30");
    // should be the same date for <=
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12-31");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018-01-01");
    // should be the same date for >=
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12-31");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017-12-31");

  }

  @Test
  public void testDayInclusive() {
    Integer date = (Integer) Literal.of("2017-12-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017-11-30");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017-12-02");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017-12-01");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017-12-01");

  }

  @Test
  public void testYearStrictLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018");
    // bound should include the same year for lower bound
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017");

  }

  @Test
  public void testYearStrictUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionStrict(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016");
    // bound should include the same year for upper bound
    assertProjectionStrict(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionStrict(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionStrict(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionStrict(spec, notEqual("date", date), Expression.Operation.NOT_EQ, "2017");

  }

  @Test
  public void testYearInclusiveLowerBound() {
    Integer date = (Integer) Literal.of("2017-01-01").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2016");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017");

  }

  @Test
  public void testYearInclusiveUpperBound() {
    Integer date = (Integer) Literal.of("2017-12-31").to(TYPE).value();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).year("date").build();

    assertProjectionInclusive(spec, lessThan("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, lessThanOrEqual("date", date), Expression.Operation.LT_EQ, "2017");
    assertProjectionInclusive(spec, greaterThan("date", date), Expression.Operation.GT_EQ, "2018");
    assertProjectionInclusive(spec, greaterThanOrEqual("date", date), Expression.Operation.GT_EQ, "2017");
    assertProjectionInclusive(spec, equal("date", date), Expression.Operation.EQ, "2017");

  }

}
