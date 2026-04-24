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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.extract;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.assertj.core.api.AbstractBooleanAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for shredded variant row-group skipping in {@link ParquetMetricsRowGroupFilter}.
 *
 * <p>Verifies that when a variant column has shredded fields (e.g. {@code $.price}), the filter can
 * use column chunk statistics to skip row groups that cannot match a predicate.
 */
class TestShreddedVariantRowGroupFilter {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestShreddedVariantRowGroupFilter.class);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "var", Types.VariantType.get()));

  private static final VariantMetadata METADATA =
      VariantMetadata.from(VariantTestUtil.createMetadata(Set.of("price", "name", "user"), true));

  private static final VariantMetadata UUID_METADATA =
      VariantMetadata.from(VariantTestUtil.createMetadata(Set.of("deviceid"), true));

  /** Price field evaluated as an integer. */
  private static final UnboundTerm<Integer> PRICE = extract("var", "$.price", "int");

  /** Name field evaluated as a string. */
  private static final UnboundTerm<String> NAME = extract("var", "$.name", "string");

  /** Device ID as a UUID. */
  private static final UnboundTerm<UUID> DEVICEID_UUID = extract("var", "$.deviceid", "uuid");

  /** Zero UUID. */
  private static final UUID UUID_ZERO = UUID.fromString("00000000-0000-0000-0000-000000000000");

  private static final UUID UUID_LOW = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final UUID UUID_MID = UUID.fromString("00000000-0000-0000-0000-000000000005");
  private static final UUID UUID_HIGH = UUID.fromString("00000000-0000-0000-0000-00000000000a");

  /** IUnknown is abvoe the high marker. */
  private static final UUID UUID_ABOVE_HIGH =
      UUID.fromString("00000000-0000-0000-C000-000000000046");

  @BeforeEach
  void before() {
    ParquetMetricsRowGroupFilter.resetShreddedMetricsCounters();
  }

  @Test
  void testShreddedIntLessThan() throws IOException {
    // Row group has prices 10..14; predicate price < 10 should skip
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(lessThan(PRICE, 10), variants))
        .as("Should skip: all prices >= 10, predicate requires price < 10")
        .isFalse();
  }

  @Test
  void testShreddedIntLessThanOverlapping() throws IOException {
    // Row group has prices 10..14; predicate price < 11 should read
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);

    assertThat(shouldRead(lessThan(PRICE, 11), variants))
        .as("Should read: price range [10,14] overlaps price < 11")
        .isTrue();
  }

  @Test
  void testShreddedIntLessThanOrEqual() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(lessThanOrEqual(PRICE, 9), variants))
        .as("Should skip: all prices >= 10, predicate requires price <= 9")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(PRICE, 10), variants))
        .as("Should read: min price == 10 matches price <= 10")
        .isTrue();
  }

  @Test
  void testShreddedIntGreaterThan() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(greaterThan(PRICE, 14), variants))
        .as("Should skip: all prices <= 14, predicate requires price > 14")
        .isFalse();

    assertThat(shouldRead(greaterThan(PRICE, 13), variants))
        .as("Should read: price range [10,14] overlaps price > 13")
        .isTrue();
  }

  @Test
  void testShreddedIntGreaterThanOrEqual() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(greaterThanOrEqual(PRICE, 15), variants))
        .as("Should skip: all prices <= 14, predicate requires price >= 15")
        .isFalse();

    assertThat(shouldRead(greaterThanOrEqual(PRICE, 14), variants))
        .as("Should read: max price == 14 matches price >= 14")
        .isTrue();
  }

  @Test
  void testShreddedIntEqual() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(equal(PRICE, 9), variants)).as("Should skip: 9 < min price 10").isFalse();
    assertThat(shouldRead(equal(PRICE, 15), variants))
        .as("Should skip: 15 > max price 14")
        .isFalse();
    assertThat(shouldRead(equal(PRICE, 12), variants))
        .as("Should read: 12 is within price range [10,14]")
        .isTrue();
  }

  @Test
  void testShreddedIsNullNoNulls() throws IOException {
    // All rows have the typed price value (no nulls in typed_value)
    List<Variant> variants = intPriceVariants(10, 11, 12);
    assertThat(shouldRead(isNull(PRICE), variants))
        .as("Should skip: all rows have typed price, IS_NULL cannot match")
        .isFalse();
  }

  @Test
  void testShreddedNotNullAllNulls() throws IOException {
    // All rows have $.price as variant null; typed_value will be null for all rows
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (int i = 0; i < 3; i++) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.ofNull()); // explicit variant null → typed_value is null
      builder.add(Variant.of(METADATA, obj));
    }
    List<Variant> variants = builder.build();

    // Shredding function says price is an int; but values are variant nulls → typed_value all null
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0));
    VariantShreddingFunction shreddingFunc =
        (id, name) -> ParquetVariantUtil.toParquetSchema(example);

    assertThat(shouldRead(notNull(PRICE), variants, shreddingFunc))
        .as("Should skip: no rows have typed price, NOT_NULL cannot match")
        .isFalse();
  }

  @Test
  void testUnshreddedPathMightMatch() throws IOException {
    // $.name is not shredded; filter should return MIGHT_MATCH (true)
    List<Variant> variants = intPriceVariants(10, 11, 12);
    // Use the price shredding function — $.name is not shredded so falls back
    assertThat(shouldRead(equal(NAME, "foo"), variants))
        .as("Should read: unshredded path must fall back to MIGHT_MATCH")
        .isTrue();
  }

  @Test
  void testNestedShreddedPath() throws IOException {
    // Shred $.user.name as a string
    ShreddedObject user1 = Variants.object(METADATA);
    user1.put("name", Variants.of("alice"));
    ShreddedObject row1 = Variants.object(METADATA);
    row1.put("user", user1);

    ShreddedObject user2 = Variants.object(METADATA);
    user2.put("name", Variants.of("bob"));
    ShreddedObject row2 = Variants.object(METADATA);
    row2.put("user", user2);

    List<Variant> variants =
        ImmutableList.of(Variant.of(METADATA, row1), Variant.of(METADATA, row2));

    // Shred $.user as an object with a shredded .name inside
    VariantShreddingFunction shreddingFunc = (id, name) -> ParquetVariantUtil.toParquetSchema(row1);

    // The path $.user is shredded but $.user.name is nested deeper —
    // the filter should handle multi-level typed_value paths correctly
    final UnboundTerm<String> username = extract("var", "$.user.name", "string");
    assertThat(shouldRead(equal(username, "charlie"), variants, shreddingFunc))
        .as("Should skip: 'charlie' is outside the [alice, bob] range")
        .isFalse();

    assertThat(shouldRead(equal(username, "alice"), variants, shreddingFunc))
        .as("Should read: 'alice' is within the range")
        .isTrue();
  }

  @Test
  void testShreddedLongPredicates() throws IOException {
    List<Variant> variants = longPriceVariants(100L, 101L, 102L, 103L, 104L);

    final UnboundTerm<Long> term = extract("var", "$.price", "long");
    assertNotRead(lessThan(term, 100L), variants, "< 100");
    assertIsRead(lessThanOrEqual(term, 100L), variants, "<= 100");
    assertIsRead(lessThan(term, 105L), variants, "< 105");
    assertIsRead(lessThan(term, 1119L), variants, "< 119L");

    assertNotRead(greaterThan(term, 104L), variants, "> 104");
    assertIsRead(greaterThanOrEqual(term, 104L), variants, " >= 104");
    assertIsRead(greaterThan(term, 103L), variants, "> 103");
    assertIsRead(greaterThan(term, 99L), variants, "> 99");

    assertNotRead(equal(term, 99L), variants, "= 99");
    assertNotRead(equal(term, 105L), variants, "= 105");
    assertIsRead(equal(term, 102L), variants, "= 102");
    assertNotRead(equal(term, 99L), variants, "= 105");
    // some not terms to see how they come out
    assertIsRead(not(equal(term, 108L)), variants, "!(= 108)");
    assertIsRead(not((greaterThan(term, 104L))), variants, "!(> 104)");
  }

  private AbstractBooleanAssert<?> assertExpression(
      Expression expr, List<Variant> variants, String text) throws IOException {
    return assertThat(shouldRead(expr, variants))
        .as(
            "Predicate '%s' on `range [%s, %s]",
            text, variants.get(0).value(), variants.get(variants.size() - 1));
  }

  private void assertIsRead(Expression expr, List<Variant> variants, String text)
      throws IOException {
    assertExpression(expr, variants, text).isTrue();
    LOG.info("Predicate '{}' succeeded", text);
  }

  private void assertNotRead(Expression expr, List<Variant> variants, String text)
      throws IOException {
    assertExpression(expr, variants, text).isFalse();
    LOG.info("Predicate '{}' succeessfully rejected", text);
  }

  @Test
  void testShreddedFloatPredicates() throws IOException {
    List<Variant> variants = floatPriceVariants(1.0F, 2.0F, 3.0F);

    final UnboundTerm<Float> term = extract("var", "$.price", "float");
    assertNotRead(lessThan(term, 1.0F), variants, "< 1.0F");
    assertNotRead(greaterThan(term, 3.0F), variants, "> 3.0F");
    // float equality is always dubious
    assertIsRead(equal(term, 2.0F), variants, "= 2.0F");
  }

  @Test
  void testShreddedFloatNaNDropsMinMax() throws IOException {
    // When any value is NaN, Parquet drops min/max → filter must return MIGHT_MATCH
    List<Variant> variants = floatPriceVariants(1.0F, Float.NaN, 3.0F);

    // Even though 99.0 is nowhere near [1.0, NaN, 3.0], stats are dropped so must read
    final UnboundTerm<Float> term = extract("var", "$.price", "float");
    assertIsRead(equal(term, 99.0F), variants, "= 99.0F with a NaN in the row group");
    assertIsRead(
        greaterThan(term, 1000.0F),
        variants,
        "> 1000; NaN in row group causes Parquet to drop min/max → MIGHT_MATCH");
  }

  @Test
  void testShreddedDoublePredicates() throws IOException {
    List<Variant> variants = doublePriceVariants(1.0D, 2.0D, 3.0D);

    final UnboundTerm<Double> term = extract("var", "$.price", "double");
    assertNotRead(lessThan(term, 1.0D), variants, "< 1.0D");
    assertNotRead(greaterThan(term, 3.0D), variants, "> 3.0D");
    assertIsRead(equal(term, 2.0D), variants, "= 2.0D");
  }

  @Test
  void testShreddedDoubleNaNDropsMinMax() throws IOException {
    // When any value is NaN, Parquet drops min/max → filter MUST return MIGHT_MATCH
    List<Variant> variants = doublePriceVariants(1.0D, Double.NaN, 3.0D);
    VariantShreddingFunction shreddingFunc = doublePriceShreddingFunc();

    final UnboundTerm<Double> term = extract("var", "$.price", "double");
    assertThat(shouldRead(equal(term, 99.0D), variants, shreddingFunc))
        .as("Should read: NaN in row group causes Parquet to drop min/max → MIGHT_MATCH")
        .isTrue();
    assertThat(shouldRead(greaterThan(term, 1000.0D), variants, shreddingFunc))
        .as("Should read: NaN in row group causes Parquet to drop min/max → MIGHT_MATCH")
        .isTrue();
  }

  @Test
  void testShreddedIntNotEqual() throws IOException {
    // NOT_EQ always returns MIGHT_MATCH
    final UnboundTerm<Integer> term = PRICE;

    assertIsRead(not(equal(term, 10)), intPriceVariants(10, 10, 10), "!(= 10) all values are 10");
    assertIsRead(
        not(equal(term, 10)), intPriceVariants(10, 11, 12, 13, 14), "!(= 10) range [10,14]");
    assertIsRead(
        not(equal(term, 99)), intPriceVariants(10, 11, 12, 13, 14), "!(= 99) range [10,14]");
  }

  @Test
  void testShreddedStringPredicates() throws IOException {
    // $.name shredded as a string field; names span "alice".."charlie"
    List<Variant> variants = nameStringVariants("alice", "bob", "charlie");
    VariantShreddingFunction shreddingFunc = nameStringShreddingFunc();

    final UnboundTerm<String> term = NAME;

    // Nothing is lexicographically less than the minimum "alice"
    assertThat(shouldRead(lessThan(term, "alice"), variants, shreddingFunc))
        .as("Should skip: 'alice' is min, nothing is < 'alice'")
        .isFalse();

    // Nothing is greater than the maximum "charlie"
    assertThat(shouldRead(greaterThan(term, "charlie"), variants, shreddingFunc))
        .as("Should skip: 'charlie' is max, nothing is > 'charlie'")
        .isFalse();

    // "david" sorts after max "charlie" → EQ cannot match
    assertThat(shouldRead(equal(term, "david"), variants, shreddingFunc))
        .as("Should skip: 'david' > max 'charlie'")
        .isFalse();

    // "aardvark" sorts before min "alice" → EQ cannot match
    assertThat(shouldRead(equal(term, "aardvark"), variants, shreddingFunc))
        .as("Should skip: 'aardvark' < min 'alice'")
        .isFalse();

    // "bob" is within [alice, charlie] → EQ might match
    assertThat(shouldRead(equal(term, "bob"), variants, shreddingFunc))
        .as("Should read: 'bob' is within [alice, charlie]")
        .isTrue();

    // GT "alice" → might match (max "charlie" > "alice")
    assertThat(shouldRead(greaterThan(term, "alice"), variants, shreddingFunc))
        .as("Should read: range [alice, charlie] overlaps > 'alice'")
        .isTrue();
  }

  @Test
  void testShreddedLiteralAllNulls() throws IOException {
    // All typed_value entries are null: any literal predicate must return CANNOT_MATCH
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (int i = 0; i < 3; i++) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.ofNull()); // variant null → typed_value written as null
      builder.add(Variant.of(METADATA, obj));
    }
    List<Variant> variants = builder.build();

    // Shredding function defines price as int so the typed_value column exists in the schema
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0));
    VariantShreddingFunction shreddingFunc =
        (id, name) -> ParquetVariantUtil.toParquetSchema(example);

    final UnboundTerm<Integer> term = PRICE;
    assertThat(shouldRead(equal(term, 5), variants, shreddingFunc))
        .as("Should skip: all typed_value are null, EQ cannot match")
        .isFalse();
    assertThat(shouldRead(greaterThan(term, 0), variants, shreddingFunc))
        .as("Should skip: all typed_value are null, GT cannot match")
        .isFalse();
    assertThat(shouldRead(lessThan(term, 100), variants, shreddingFunc))
        .as("Should skip: all typed_value are null, LT cannot match")
        .isFalse();
  }

  @Test
  void testShreddedIsNullSomeNulls() throws IOException {
    // Mix of typed_value (non-null) and null typed_value — IS_NULL should read
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0));
    VariantShreddingFunction shreddingFunc =
        (id, name) -> ParquetVariantUtil.toParquetSchema(example);

    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (int price : new int[] {10, 11, 12}) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.of(price));
      builder.add(Variant.of(METADATA, obj));
    }
    for (int i = 0; i < 2; i++) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.ofNull()); // null typed_value
      builder.add(Variant.of(METADATA, obj));
    }

    assertThat(shouldRead(isNull(PRICE), builder.build(), shreddingFunc))
        .as("Should read: some rows have null typed_value, IS_NULL might match")
        .isTrue();
  }

  @Test
  void testShreddedNotNullSomeValues() throws IOException {
    // Mix of typed_value (non-null) and null typed_value — NOT_NULL should read
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0));
    VariantShreddingFunction shreddingFunc =
        (id, name) -> ParquetVariantUtil.toParquetSchema(example);

    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (int price : new int[] {10, 11, 12}) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.of(price));
      builder.add(Variant.of(METADATA, obj));
    }
    for (int i = 0; i < 2; i++) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.ofNull()); // null typed_value
      builder.add(Variant.of(METADATA, obj));
    }

    assertThat(shouldRead(notNull(PRICE), builder.build(), shreddingFunc))
        .as("Should read: some rows have non-null typed_value, NOT_NULL might match")
        .isTrue();
  }

  @Test
  void testShreddedStringMaxIsAllMaxCodepoints() throws IOException {
    // U+10FFFF is the highest Unicode code point; its UTF-8 encoding ends in bytes 0xBF 0xBF,
    // which approach the 0xFF boundary. A string of 17 such characters is longer than the
    // Iceberg-metrics truncation limit of 16 code points, so UnicodeUtil.truncateStringMax
    // returns null (no valid upper bound can be computed by incrementing any code point because
    // every code point is already at the maximum). This tests that the Parquet column chunk
    // statistics — which store the exact bytes, not a truncated approximation — still allow the
    // filter to make correct skip decisions.
    final String maxCodepoint = "\uDBFF\uDFFF"; // U+10FFFF as a Java surrogate pair
    final String allMax = maxCodepoint.repeat(17); // > 16 code points: truncation returns null
    List<Variant> variants = nameStringVariants("alpha", allMax);
    VariantShreddingFunction shreddingFunc = nameStringShreddingFunc();

    final UnboundTerm<String> term = NAME;

    // Exact Parquet stats: max is known, so GT(max) can skip
    assertThat(shouldRead(greaterThan(term, allMax), variants, shreddingFunc))
        .as("Should skip: nothing is greater than the all-U+10FFFF max")
        .isFalse();

    // EQ at the exact max: value is present, cannot skip
    assertThat(shouldRead(equal(term, allMax), variants, shreddingFunc))
        .as("Should read: all-U+10FFFF string is the max value")
        .isTrue();

    // One extra U+10FFFF pushes the literal beyond the stored max
    assertThat(shouldRead(equal(term, allMax + maxCodepoint), variants, shreddingFunc))
        .as("Should skip: literal is strictly greater than the max")
        .isFalse();

    // LT at min: nothing below "alpha"
    assertThat(shouldRead(lessThan(term, "alpha"), variants, shreddingFunc))
        .as("Should skip: nothing is less than min 'alpha'")
        .isFalse();

    // Mid-range value: within [alpha, allMax]
    assertThat(shouldRead(equal(term, "beta"), variants, shreddingFunc))
        .as("Should read: 'beta' is within [alpha, all-U+10FFFF]")
        .isTrue();
  }

  @Test
  void testShreddedSingleValueAtExactBoundary() throws IOException {
    // When a row group contains only one distinct value (min == max), predicates at that exact
    // boundary must be evaluated correctly: LT and GT should skip (the value cannot satisfy them),
    // while LT_EQ, GT_EQ, and EQ should read (the value might satisfy them).
    List<Variant> variants = intPriceVariants(10, 10, 10); // all rows have price = 10
    final UnboundTerm<Integer> term = PRICE;

    // min=10, LT(10): minVsLiteral=0, 0 < 0 is false → CANNOT_MATCH
    assertNotRead(lessThan(term, 10), variants, "< 10 when all values are 10");
    // max=10, GT(10): literalVsMax=0, 0 < 0 is false → CANNOT_MATCH
    assertNotRead(greaterThan(term, 10), variants, "> 10 when all values are 10");
    // min=10, LT_EQ(10): minVsLiteral=0, 0 <= 0 is true → MIGHT_MATCH
    assertIsRead(lessThanOrEqual(term, 10), variants, "<= 10 when all values are 10");
    // max=10, GT_EQ(10): literalVsMax=0, 0 <= 0 is true → MIGHT_MATCH
    assertIsRead(greaterThanOrEqual(term, 10), variants, ">= 10 when all values are 10");
    // EQ(10): both comparisons are 0 → MIGHT_MATCH
    assertIsRead(equal(term, 10), variants, "= 10 when all values are 10");
  }

  @Test
  void testShreddedAndCompoundPredicate() throws IOException {
    // AND over two variant extract predicates: the row group can be skipped if either arm cannot
    // match, and must be read only when both arms might match.
    List<Variant> variants = intPriceVariants(10, 11, 12); // min=10, max=12
    final UnboundTerm<Integer> term = PRICE;

    // Both arms overlap the range [10,12] → MIGHT_MATCH
    assertIsRead(
        and(greaterThan(term, 5), lessThan(term, 15)), variants, "5 < price < 15 overlaps [10,12]");
    // two predicates will be evaluated here, min and max.
    // this highlights at an in-range query will be expensive on unshredded numbers
    // as they will need to be read twice.
    assertShreddedMetricsProcessed(2);

    // GT(20) cannot match (max=12 < 20) → AND short-circuits to CANNOT_MATCH
    assertNotRead(
        and(greaterThan(term, 20), lessThan(term, 25)),
        variants,
        "20 < price < 25 is above the range [10,12]");

    // LT(8) cannot match (min=10 > 8) → AND short-circuits to CANNOT_MATCH
    assertNotRead(
        and(greaterThan(term, 5), lessThan(term, 8)),
        variants,
        "5 < price < 8 is below the range [10,12]");
  }

  @Test
  void testShreddedSetMembership() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12); // min=10, max=12
    final UnboundTerm<Integer> term = PRICE;

    // All set values are below the range — row group can be skipped
    assertNotRead(in(term, 1, 2, 3), variants, "IN {1,2,3} with range [10..12]");
    assertShreddedMetricsProcessed(1);

    // All set values are above the range — row group can be skipped
    assertNotRead(in(term, 20, 30), variants, "IN {20,30} with range [10..12]");
    // Single value that is below the range — row group can be skipped
    assertNotRead(in(term, 5), variants, "IN {5} with range [10..12]");
    assertNotRead(in(term, -100, 400), variants, "IN {-100, 400} with range [10..12]");
    // At least one set value is within the range — row group must be read
    assertIsRead(in(term, 9, 10), variants, "IN {9,10} with range [10..12]");
    assertIsRead(in(term, 12, 13), variants, "IN {12,13} with range [10..12]");
    assertIsRead(in(term, 10, 11, 12), variants, "IN {10,11,12} with range [10..12]");
    // Exact boundary matches — row group must be read
    assertIsRead(in(term, 10), variants, "IN {10} with range [10..12]");
    assertIsRead(in(term, 12), variants, "IN {12} with range [10..12]");
  }

  @Test
  void testShreddedUUIDEqual() throws IOException {
    // Row group has deviceid range [UUID_LOW, UUID_HIGH]
    List<Variant> variants = uuidDeviceIdVariants(UUID_LOW, UUID_MID, UUID_HIGH);
    final UnboundTerm<UUID> term = DEVICEID_UUID;

    // EQ with a value strictly below the range — row group can be skipped
    assertNotRead(equal(term, UUID_ZERO), variants, "= nil UUID, range [LOW, HIGH]");

    // EQ with a value strictly above the range — row group can be skipped
    assertNotRead(equal(term, UUID_ABOVE_HIGH), variants, "= UUID 100, range [LOW, HIGH]");

    // EQ with a value within the range — row group must be read
    assertIsRead(equal(term, UUID_MID), variants, "= UUID_MID within [LOW, HIGH]");

    // EQ with exact boundary values — row group must be read
    assertIsRead(equal(term, UUID_LOW), variants, "= UUID_LOW, lower boundary");
    assertIsRead(equal(term, UUID_HIGH), variants, "= UUID_HIGH, upper boundary");
  }

  @Test
  void testShreddedUUIDNotEqual() throws IOException {
    List<Variant> variants = uuidDeviceIdVariants(UUID_LOW, UUID_MID, UUID_HIGH);
    final UnboundTerm<UUID> term = DEVICEID_UUID;

    // NOT_EQ never uses min/max to skip — always MIGHT_MATCH
    assertIsRead(not(equal(term, UUID_MID)), variants, "!= UUID_MID with range [LOW, HIGH]");
    assertIsRead(
        not(equal(term, UUID_MID)),
        uuidDeviceIdVariants(UUID_MID, UUID_MID, UUID_MID),
        "!= UUID_MID, all values equal UUID_MID");
  }

  @Test
  void testShreddedUUIDIn() throws IOException {
    ParquetMetricsRowGroupFilter.resetShreddedMetricsCounters();

    // Row group has deviceid range [UUID_LOW, UUID_HIGH]
    List<Variant> variants = uuidDeviceIdVariants(UUID_LOW, UUID_MID, UUID_HIGH);
    final UnboundTerm<UUID> term = DEVICEID_UUID;

    // All set values are below the range — row group can be skipped
    assertNotRead(in(term, UUID_ZERO), variants, "IN {nil UUID}, range [LOW, HIGH]");
    int expected = 1;
    assertShreddedMetricsProcessed(expected++);

    // All set values are above the range — row group can be skipped
    assertNotRead(in(term, UUID_ABOVE_HIGH), variants, "IN {UUID 100}, range [LOW, HIGH]");
    assertShreddedMetricsProcessed(expected++);

    assertNotRead(
        in(term, UUID_ABOVE_HIGH, UUID.fromString("00000000-0000-0000-0000-0000000000c8")),
        variants,
        "IN {UUID 100, UUID 200}, range [LOW, HIGH]");
    assertShreddedMetricsProcessed(expected++);

    // At least one set value is within the range — row group must be read
    assertIsRead(in(term, UUID_MID), variants, "IN {UUID_MID} within [LOW, HIGH]");
    assertShreddedMetricsProcessed(expected++);
    assertIsRead(
        in(term, UUID_ZERO, UUID_MID), variants, "IN {below, UUID_MID} straddles [LOW, HIGH]");
    assertShreddedMetricsProcessed(expected++);
    assertIsRead(
        in(term, UUID_LOW, UUID_MID, UUID_HIGH),
        variants,
        "IN {LOW, MID, HIGH} covers [LOW, HIGH]");
    assertShreddedMetricsProcessed(expected++);

    // Exact boundary values — row group must be read
    assertIsRead(in(term, UUID_LOW), variants, "IN {UUID_LOW}, lower boundary");
    assertShreddedMetricsProcessed(expected++);
    assertIsRead(in(term, UUID_HIGH), variants, "IN {UUID_HIGH}, upper boundary");
    assertShreddedMetricsProcessed(expected++);
  }

  // ---------------------------------------------------------------------------
  // Skip-counter tests: prove the filter is actually skipping row groups
  // ---------------------------------------------------------------------------

  @Test
  void testSkipCounterLessThanSkips() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(lessThan(PRICE, 10), variants)).isFalse();
    assertShreddedSkipped(1);
  }

  @Test
  void testSkipCounterGreaterThanSkips() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(greaterThan(PRICE, 14), variants)).isFalse();
    assertShreddedSkipped(1);
  }

  @Test
  void testSkipCounterEqualBelowRangeSkips() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(equal(PRICE, 5), variants)).isFalse();
    assertShreddedSkipped(1);
  }

  @Test
  void testSkipCounterEqualAboveRangeSkips() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(equal(PRICE, 99), variants)).isFalse();
    assertShreddedSkipped(1);
  }

  @Test
  void testSkipCounterEqualInRangeDoesNotSkip() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12, 13, 14);
    assertThat(shouldRead(equal(PRICE, 12), variants)).isTrue();
    // examined > 0, but no skip
    assertShreddedSkipped(0);
    assertShreddedMetricsProcessed(1);
  }

  @Test
  void testSkipCounterIsNullWithNoNullsSkips() throws IOException {
    List<Variant> variants = intPriceVariants(10, 11, 12);
    assertThat(shouldRead(isNull(PRICE), variants)).isFalse();
    assertShreddedSkipped(1);
  }

  @Test
  void testSkipCounterNotNullAllNullsSkips() throws IOException {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (int i = 0; i < 3; i++) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.ofNull());
      builder.add(Variant.of(METADATA, obj));
    }
    List<Variant> variants = builder.build();
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0));
    VariantShreddingFunction shreddingFunc =
        (id, name) -> ParquetVariantUtil.toParquetSchema(example);

    assertThat(shouldRead(notNull(PRICE), variants, shreddingFunc)).isFalse();
    assertShreddedSkipped(1);
  }

  @Test
  void testSkipCounterUnshreddedPathDoesNotSkip() throws IOException {
    // $.name isn't in the shredded schema → falls back to MIGHT_MATCH before consulting stats →
    // the skipped counter must NOT advance (we don't count fallbacks as skips).
    List<Variant> variants = intPriceVariants(10, 11, 12);
    assertThat(shouldRead(equal(NAME, "foo"), variants)).isTrue();
    assertShreddedSkipped(0);
  }

  // --- helpers ---

  private static void assertShreddedMetricsProcessed(final int expected) {
    assertThat(ParquetMetricsRowGroupFilter.variantPredicatesShreddedMetricsEvaluated())
        .describedAs("Count of shredded metrics filtered on in predicates")
        .isEqualTo(expected);
  }

  private static void assertShreddedSkipped(final long expected) {
    assertThat(ParquetMetricsRowGroupFilter.variantPredicatesShreddedSkipped())
        .describedAs("Count of row groups skipped by shredded variant predicates")
        .isEqualTo(expected);
  }

  private List<Variant> intPriceVariants(int... prices) {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (int price : prices) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.of(price));
      builder.add(Variant.of(METADATA, obj));
    }
    return builder.build();
  }

  private List<Variant> longPriceVariants(long... prices) {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (long price : prices) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.of(price));
      builder.add(Variant.of(METADATA, obj));
    }
    return builder.build();
  }

  private List<Variant> floatPriceVariants(float... prices) {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (float price : prices) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.of(price));
      builder.add(Variant.of(METADATA, obj));
    }
    return builder.build();
  }

  private List<Variant> doublePriceVariants(double... prices) {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (double price : prices) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("price", Variants.of(price));
      builder.add(Variant.of(METADATA, obj));
    }
    return builder.build();
  }

  private VariantShreddingFunction floatPriceShreddingFunc() {
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0.0F));
    return (id, name) -> ParquetVariantUtil.toParquetSchema(example);
  }

  private VariantShreddingFunction doublePriceShreddingFunc() {
    ShreddedObject example = Variants.object(METADATA);
    example.put("price", Variants.of(0.0D));
    return (id, name) -> ParquetVariantUtil.toParquetSchema(example);
  }

  private List<Variant> nameStringVariants(String... names) {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (String name : names) {
      ShreddedObject obj = Variants.object(METADATA);
      obj.put("name", Variants.of(name));
      builder.add(Variant.of(METADATA, obj));
    }
    return builder.build();
  }

  private VariantShreddingFunction nameStringShreddingFunc() {
    ShreddedObject example = Variants.object(METADATA);
    example.put("name", Variants.of("x"));
    return (id, name) -> ParquetVariantUtil.toParquetSchema(example);
  }

  private List<Variant> uuidDeviceIdVariants(UUID... uuids) {
    ImmutableList.Builder<Variant> builder = ImmutableList.builder();
    for (UUID uuid : uuids) {
      ShreddedObject obj = Variants.object(UUID_METADATA);
      obj.put("deviceid", Variants.ofUUID(uuid));
      builder.add(Variant.of(UUID_METADATA, obj));
    }
    return builder.build();
  }

  /**
   * Should the expression, when evaluated againt the fully shredded set of veriants, require the
   * RowGroup to be read?
   *
   * @param expr expression
   * @param variants list of variants
   */
  private boolean shouldRead(Expression expr, List<Variant> variants) throws IOException {
    // Derive the shredding schema from the first variant's structure
    VariantShreddingFunction shreddingFunc =
        (id, name) -> ParquetVariantUtil.toParquetSchema(variants.get(0).value());
    return shouldRead(expr, variants, shreddingFunc);
  }

  /**
   * Should a set of variants, shred with the supplied shredding function, be read?
   *
   * @param expr expression
   * @param variants list of variants
   * @param shreddingFunc shredding function
   * @return true if a file containing only these variants should be read.
   */
  private boolean shouldRead(
      Expression expr, List<Variant> variants, VariantShreddingFunction shreddingFunc)
      throws IOException {
    OutputFile out = new InMemoryOutputFile();
    GenericRecord record = GenericRecord.create(SCHEMA);

    FileAppender<Record> writer =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc(shreddingFunc)
            .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
            .build();

    try (writer) {
      for (int i = 0; i < variants.size(); i++) {
        record.setField("id", (long) i);
        record.setField("var", variants.get(i));
        writer.add(record);
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(out.toInputFile()))) {
      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      return new ParquetMetricsRowGroupFilter(SCHEMA, expr, true).shouldRead(fileSchema, rowGroup);
    }
  }
}
