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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestExpressionParserWithIDReference {

  private static final Types.StructType STRUCT_TYPE =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withoutZone()),
          required(110, "s", Types.StringType.get()),
          required(111, "uuid", Types.UUIDType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)));

  private static final Schema SCHEMA = new Schema(STRUCT_TYPE.fields());

  @Test
  public void testIDReferenceExpressionSerialization() {
    // Create expressions using IDReference
    Expression[] resolvedExpressions =
        new Expression[] {
          Expressions.equal(Expressions.ref("id", 100), 42L),
          Expressions.lessThan(Expressions.ref("data", 101), "test"),
          Expressions.greaterThanOrEqual(Expressions.ref("i", 103), 10),
          Expressions.isNull(Expressions.ref("f", 105)),
          Expressions.notNull(Expressions.ref("date", 107)),
          Expressions.startsWith(Expressions.ref("s", 110), "prefix"),
          Expressions.in(Expressions.ref("l", 104), 1L, 2L, 3L),
          Expressions.notIn(Expressions.ref("b", 102), true, false),
          Expressions.isNaN(Expressions.ref("d", 106)),
          Expressions.notNaN(Expressions.ref("f", 105))
        };

    for (Expression expr : resolvedExpressions) {
      // Verify the expression uses IDReference
      assertThat(expr).isInstanceOf(UnboundPredicate.class);
      UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
      assertThat(predicate.term()).isInstanceOf(IDReference.class);

      // Test JSON serialization
      String json = ExpressionParser.toJson(expr, true);
      assertThat(json).isNotNull();
      assertThat(json).contains("\"type\"");

      // Test that JSON contains the field name (not field ID since parser doesn't support it yet)
      IDReference<?> resolvedRef = (IDReference<?>) predicate.term();
      assertThat(json).contains(resolvedRef.name());
    }
  }

  @Test
  public void testIDReferenceRoundTripCompatibility() {
    // Test that IDReference expressions can be serialized and parsed back
    Expression resolvedExpr = Expressions.equal(Expressions.ref("id", 100), 42L);
    Expression namedExpr = Expressions.equal(Expressions.ref("id"), 42L);

    // Both should produce the same JSON since parser only uses names
    String resolvedJson = ExpressionParser.toJson(resolvedExpr, true);
    String namedJson = ExpressionParser.toJson(namedExpr, true);
    assertThat(resolvedJson).isEqualTo(namedJson);

    // Parse back and verify equivalence
    Expression parsedFromResolved = ExpressionParser.fromJson(resolvedJson, SCHEMA);
    Expression parsedFromNamed = ExpressionParser.fromJson(namedJson, SCHEMA);

    // Both parsed expressions should be equivalent
    assertThat(ExpressionUtil.equivalent(parsedFromResolved, parsedFromNamed, STRUCT_TYPE, true))
        .isTrue();

    // The parsed expression should be equivalent to the original named reference expression
    assertThat(ExpressionUtil.equivalent(namedExpr, parsedFromResolved, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testIDReferenceComplexExpressions() {
    // Test complex expressions with IDReference
    Expression complexExpr =
        Expressions.and(
            Expressions.or(
                Expressions.equal(Expressions.ref("data", 101), "test"),
                Expressions.isNull(Expressions.ref("data", 101))),
            Expressions.greaterThanOrEqual(Expressions.ref("id", 100), 100L));

    // Serialize to JSON using toJson (NAMES_ONLY mode - no field IDs)
    String json = ExpressionParser.toJson(complexExpr, true);
    assertThat(json).contains("\"type\" : \"and\"");
    assertThat(json).contains("\"type\" : \"or\"");
    assertThat(json).contains("\"data\"");
    // toJson uses NAMES_ONLY mode, so source-id should NOT be included
    assertThat(json).doesNotContain("\"source-id\"");

    // Parse back
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);

    // Create equivalent expression with NamedReference for comparison
    Expression namedEquivalent =
        Expressions.and(
            Expressions.or(Expressions.equal("data", "test"), Expressions.isNull("data")),
            Expressions.greaterThanOrEqual("id", 100L));

    // Should be equivalent
    assertThat(ExpressionUtil.equivalent(namedEquivalent, parsed, STRUCT_TYPE, true)).isTrue();

    // Serialize using toResolvedJson (WITH_FIELD_IDS mode - includes field IDs)
    String resolvedJson = ExpressionParser.toResolvedJson(complexExpr, true);
    assertThat(resolvedJson).contains("\"type\" : \"and\"");
    assertThat(resolvedJson).contains("\"type\" : \"or\"");
    assertThat(resolvedJson).contains("\"data\"");
    assertThat(resolvedJson).contains("\"source-id\"");

    // Parse back the resolved JSON
    Expression parsedResolved = ExpressionParser.fromJson(resolvedJson, SCHEMA);

    // Should also be equivalent
    assertThat(ExpressionUtil.equivalent(namedEquivalent, parsedResolved, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testIDReferenceTransformExpressions() {
    // Test transform expressions - using NamedReference for transforms since
    // IDReference needs proper type parameters for transform methods
    Expression dayTransform = Expressions.equal(Expressions.day("date"), "2023-01-15");
    Expression bucketTransform = Expressions.equal(Expressions.bucket("id", 10), 5);

    // Test serialization
    String dayJson = ExpressionParser.toJson(dayTransform, true);
    String bucketJson = ExpressionParser.toJson(bucketTransform, true);

    assertThat(dayJson).contains("\"transform\" : \"day\"");
    assertThat(dayJson).contains("\"term\" : \"date\"");
    assertThat(bucketJson).contains("\"transform\" : \"bucket[10]\"");
    assertThat(bucketJson).contains("\"term\" : \"id\"");

    // Test round-trip
    Expression parsedDay = ExpressionParser.fromJson(dayJson, SCHEMA);
    Expression parsedBucket = ExpressionParser.fromJson(bucketJson, SCHEMA);

    // Should maintain equivalence after round-trip
    assertThat(ExpressionUtil.equivalent(dayTransform, parsedDay, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(bucketTransform, parsedBucket, STRUCT_TYPE, true))
        .isTrue();
  }

  @Test
  public void testResolvedReferenceBindingAfterParsing() {
    // Test that expressions with IDReference bind correctly after parsing
    Expression original = Expressions.equal(Expressions.ref("id", 100), 42L);

    // Serialize and parse
    String json = ExpressionParser.toJson(original, true);
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);

    // Both should bind successfully
    Expression originalBound = Binder.bind(STRUCT_TYPE, original, true);
    Expression parsedBound = Binder.bind(STRUCT_TYPE, parsed, true);

    // Both bound expressions should be identical
    assertThat(originalBound).isInstanceOf(BoundPredicate.class);
    assertThat(parsedBound).isInstanceOf(BoundPredicate.class);

    BoundPredicate<?> originalBoundPred = (BoundPredicate<?>) originalBound;
    BoundPredicate<?> parsedBoundPred = (BoundPredicate<?>) parsedBound;

    // Should reference the same field
    assertThat(originalBoundPred.ref().fieldId()).isEqualTo(100);
    assertThat(parsedBoundPred.ref().fieldId()).isEqualTo(100);
    assertThat(originalBoundPred.ref().name()).isEqualTo("id");
    assertThat(parsedBoundPred.ref().name()).isEqualTo("id");
  }

  @Test
  public void testIDReferenceWithDifferentTypes() {
    // Test IDReference with various data types
    Expression[] typedExpressions =
        new Expression[] {
          Expressions.equal(Expressions.ref("b", 102), true),
          Expressions.equal(Expressions.ref("i", 103), 42),
          Expressions.equal(Expressions.ref("l", 104), 42L),
          Expressions.equal(Expressions.ref("f", 105), 3.14f),
          Expressions.equal(Expressions.ref("d", 106), 3.14159),
          Expressions.equal(Expressions.ref("s", 110), "test string"),
          Expressions.equal(Expressions.ref("uuid", 111), UUID.randomUUID()),
          Expressions.equal(Expressions.ref("dec_11_2", 115), new BigDecimal("123.45"))
        };

    for (Expression expr : typedExpressions) {
      // Test serialization doesn't break with different types
      String json = ExpressionParser.toJson(expr, true);
      assertThat(json).isNotNull();

      // Test parsing back
      Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
      assertThat(parsed).isNotNull();

      // Test binding
      Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
      assertThat(bound).isInstanceOf(BoundPredicate.class);
    }
  }

  @Test
  public void testIDReferenceJsonStructure() {
    // Test the exact JSON structure produced by IDReference
    Expression expr = Expressions.equal(Expressions.ref("data", 101), "test");
    String json = ExpressionParser.toJson(expr, true);

    // The JSON should look like a regular reference since parser doesn't support field IDs yet
    String expectedStructure =
        "{\n"
            + "  \"type\" : \"eq\",\n"
            + "  \"term\" : \"data\",\n"
            + "  \"value\" : \"test\"\n"
            + "}";

    assertThat(json).isEqualTo(expectedStructure);

    // Verify it parses back correctly
    Expression parsed = ExpressionParser.fromJson(json);
    assertThat(parsed).isInstanceOf(UnboundPredicate.class);

    UnboundPredicate<?> predicate = (UnboundPredicate<?>) parsed;
    assertThat(predicate.term()).isInstanceOf(NamedReference.class);
    assertThat(predicate.term().ref().name()).isEqualTo("data");
  }

  @Test
  public void testIDReferenceEquivalenceAfterSerialization() {
    // Test that IDReference expressions maintain equivalence after serialization
    Expression resolvedExpr =
        Expressions.and(
            Expressions.greaterThan(Expressions.ref("id", 100), 50L),
            Expressions.lessThan(Expressions.ref("id", 100), 200L));

    Expression namedExpr =
        Expressions.and(Expressions.greaterThan("id", 50L), Expressions.lessThan("id", 200L));

    // Serialize both
    String resolvedJson = ExpressionParser.toJson(resolvedExpr, true);
    String namedJson = ExpressionParser.toJson(namedExpr, true);

    // Should produce identical JSON
    assertThat(resolvedJson).isEqualTo(namedJson);

    // Parse both back
    Expression parsedResolved = ExpressionParser.fromJson(resolvedJson, SCHEMA);
    Expression parsedNamed = ExpressionParser.fromJson(namedJson, SCHEMA);

    // All should be equivalent
    assertThat(ExpressionUtil.equivalent(resolvedExpr, namedExpr, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(parsedResolved, parsedNamed, STRUCT_TYPE, true)).isTrue();
    assertThat(ExpressionUtil.equivalent(resolvedExpr, parsedResolved, STRUCT_TYPE, true)).isTrue();
  }

  @Test
  public void testIDReferenceTransformExpressionEquivalence() {
    // Test expressions that reference transforms where the transform terms are equivalent to
    // resolved references
    // Since UnboundTransform only accepts NamedReference, we test equivalence through
    // binding/unbinding

    // Create transform expressions using NamedReference (current approach)
    Expression dayTransformNamed = Expressions.equal(Expressions.day("date"), "2023-01-15");
    Expression bucketTransformNamed = Expressions.equal(Expressions.bucket("id", 10), 5);
    Expression truncateTransformNamed = Expressions.equal(Expressions.truncate("data", 4), "test");

    // Create equivalent expressions using IDReference for the predicate terms
    Expression dayWithResolvedRef = Expressions.equal(Expressions.ref("date", 107), "2023-01-15");
    Expression bucketWithResolvedRef = Expressions.equal(Expressions.ref("id", 100), 5L);
    Expression truncateWithResolvedRef = Expressions.equal(Expressions.ref("data", 101), "test");

    // Bind all expressions
    Expression boundDayTransform = Binder.bind(STRUCT_TYPE, dayTransformNamed, true);
    Expression boundBucketTransform = Binder.bind(STRUCT_TYPE, bucketTransformNamed, true);
    Expression boundTruncateTransform = Binder.bind(STRUCT_TYPE, truncateTransformNamed, true);

    Expression boundDayResolved = Binder.bind(STRUCT_TYPE, dayWithResolvedRef, true);
    Expression boundBucketResolved = Binder.bind(STRUCT_TYPE, bucketWithResolvedRef, true);
    Expression boundTruncateResolved = Binder.bind(STRUCT_TYPE, truncateWithResolvedRef, true);

    // Verify all expressions bound successfully
    assertThat(boundDayTransform).isInstanceOf(BoundPredicate.class);
    assertThat(boundBucketTransform).isInstanceOf(BoundPredicate.class);
    assertThat(boundTruncateTransform).isInstanceOf(BoundPredicate.class);
    assertThat(boundDayResolved).isInstanceOf(BoundPredicate.class);
    assertThat(boundBucketResolved).isInstanceOf(BoundPredicate.class);
    assertThat(boundTruncateResolved).isInstanceOf(BoundPredicate.class);

    // Test transform expressions in complex expressions with resolved references
    Expression complexTransformExpr =
        Expressions.and(
            Expressions.equal(Expressions.day("date"), "2023-01-15"),
            Expressions.equal(Expressions.ref("id", 100), 42L));

    Expression boundComplexExpr = Binder.bind(STRUCT_TYPE, complexTransformExpr, true);
    assertThat(boundComplexExpr).isInstanceOf(And.class);

    // Verify serialization works for transform expressions that coexist with resolved references
    String complexJson = ExpressionParser.toJson(complexTransformExpr, true);
    assertThat(complexJson).contains("\"transform\" : \"day\"");
    assertThat(complexJson).contains("\"term\" : \"date\"");
    assertThat(complexJson).contains("\"term\" : \"id\"");

    // Verify round-trip maintains correctness
    Expression parsedComplex = ExpressionParser.fromJson(complexJson, SCHEMA);
    Expression boundParsedComplex = Binder.bind(STRUCT_TYPE, parsedComplex, true);

    // Both bound expressions should reference the same fields
    assertThat(boundComplexExpr.toString()).isEqualTo(boundParsedComplex.toString());
  }

  @Test
  public void testIDReferenceInComplexTransformExpressions() {
    // Test complex expressions that combine transforms with resolved references
    Expression complexExpr =
        Expressions.or(
            Expressions.and(
                Expressions.equal(Expressions.bucket("id", 8), 3),
                Expressions.equal(Expressions.ref("data", 101), "test")),
            Expressions.and(
                Expressions.equal(Expressions.day("date"), "2023-01-15"),
                Expressions.isNull(Expressions.ref("f", 105))));

    // Test serialization
    String json = ExpressionParser.toJson(complexExpr, true);
    assertThat(json).contains("\"transform\" : \"bucket[8]\"");
    assertThat(json).contains("\"transform\" : \"day\"");
    assertThat(json).contains("\"term\" : \"id\"");
    assertThat(json).contains("\"term\" : \"date\"");
    assertThat(json).contains("\"term\" : \"data\"");
    assertThat(json).contains("\"term\" : \"f\"");

    // Test that parsing back maintains structure
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    assertThat(parsed).isInstanceOf(Or.class);

    // Test binding works correctly
    Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
    assertThat(bound).isInstanceOf(Or.class);

    // Verify equivalence with original
    assertThat(ExpressionUtil.equivalent(complexExpr, parsed, STRUCT_TYPE, true)).isTrue();

    // Test that mixed transform and resolved reference expressions bind to same fields
    Expression originalBound = Binder.bind(STRUCT_TYPE, complexExpr, true);

    // Both bound expressions should be structurally equivalent
    assertThat(originalBound.toString()).isEqualTo(bound.toString());
  }

  @Test
  public void testComplexExpressionsWithIDReferenceTransforms() {
    // Test complex expressions combining transforms created from IDReference
    Expression complexExpr =
        Expressions.and(
            Expressions.or(
                Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
                Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15")),
            Expressions.and(
                Expressions.equal(Expressions.truncate(Expressions.ref("data", 101), 4), "test"),
                Expressions.isNull(Expressions.ref("f", 105))));

    // Test serialization of complex expression
    String json = ExpressionParser.toJson(complexExpr, true);

    // Verify all transforms and terms are present in JSON
    assertThat(json).contains("\"transform\" : \"bucket[8]\"");
    assertThat(json).contains("\"transform\" : \"day\"");
    assertThat(json).contains("\"transform\" : \"truncate[4]\"");
    assertThat(json).contains("\"term\" : \"id\"");
    assertThat(json).contains("\"term\" : \"date\"");
    assertThat(json).contains("\"term\" : \"data\"");
    assertThat(json).contains("\"term\" : \"f\"");

    // Test parsing back maintains structure
    Expression parsed = ExpressionParser.fromJson(json, SCHEMA);
    assertThat(parsed).isInstanceOf(And.class);

    // Test binding works correctly for the complex expression
    Expression bound = Binder.bind(STRUCT_TYPE, parsed, true);
    assertThat(bound).isInstanceOf(And.class);

    // Verify equivalence after round-trip
    assertThat(ExpressionUtil.equivalent(complexExpr, parsed, STRUCT_TYPE, true)).isTrue();

    // Test that the bound expression maintains correct structure
    Expression originalBound = Binder.bind(STRUCT_TYPE, complexExpr, true);
    assertThat(originalBound.toString()).isEqualTo(bound.toString());
  }

  @Test
  public void testIDReferenceTransformCompatibilityWithNamedReference() {
    // Test that transforms created with IDReference are equivalent to those created with
    // NamedReference

    // Create equivalent transforms using both approaches
    Expression bucketWithResolved =
        Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3);
    Expression bucketWithNamed = Expressions.equal(Expressions.bucket("id", 8), 3);

    Expression dayWithResolved =
        Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15");
    Expression dayWithNamed = Expressions.equal(Expressions.day("date"), "2023-01-15");

    // Test that both serialize to the same JSON
    String resolvedBucketJson = ExpressionParser.toJson(bucketWithResolved, true);
    String namedBucketJson = ExpressionParser.toJson(bucketWithNamed, true);
    String resolvedDayJson = ExpressionParser.toJson(dayWithResolved, true);
    String namedDayJson = ExpressionParser.toJson(dayWithNamed, true);

    assertThat(resolvedBucketJson).isEqualTo(namedBucketJson);
    assertThat(resolvedDayJson).isEqualTo(namedDayJson);

    // Test that parsing back produces equivalent expressions
    Expression parsedResolvedBucket = ExpressionParser.fromJson(resolvedBucketJson, SCHEMA);
    Expression parsedNamedBucket = ExpressionParser.fromJson(namedBucketJson, SCHEMA);
    Expression parsedResolvedDay = ExpressionParser.fromJson(resolvedDayJson, SCHEMA);
    Expression parsedNamedDay = ExpressionParser.fromJson(namedDayJson, SCHEMA);

    // All should be equivalent
    assertThat(ExpressionUtil.equivalent(bucketWithResolved, bucketWithNamed, STRUCT_TYPE, true))
        .isTrue();
    assertThat(ExpressionUtil.equivalent(dayWithResolved, dayWithNamed, STRUCT_TYPE, true))
        .isTrue();
    assertThat(
            ExpressionUtil.equivalent(parsedResolvedBucket, parsedNamedBucket, STRUCT_TYPE, true))
        .isTrue();
    assertThat(ExpressionUtil.equivalent(parsedResolvedDay, parsedNamedDay, STRUCT_TYPE, true))
        .isTrue();

    // Test that binding produces identical results
    Expression boundResolvedBucket = Binder.bind(STRUCT_TYPE, parsedResolvedBucket, true);
    Expression boundNamedBucket = Binder.bind(STRUCT_TYPE, parsedNamedBucket, true);
    Expression boundResolvedDay = Binder.bind(STRUCT_TYPE, parsedResolvedDay, true);
    Expression boundNamedDay = Binder.bind(STRUCT_TYPE, parsedNamedDay, true);

    assertThat(boundResolvedBucket.toString()).isEqualTo(boundNamedBucket.toString());
    assertThat(boundResolvedDay.toString()).isEqualTo(boundNamedDay.toString());
  }

  @Test
  public void testBoundExpressionSerializationWithIDReference() {
    // Test that bound expressions serialize to JSON with IDReference terms when
    // includeFieldIds=true

    // Create and bind simple expressions
    Expression simpleExpr = Expressions.equal(Expressions.ref("id", 100), 42L);
    Expression boundSimple = Binder.bind(STRUCT_TYPE, simpleExpr, true);

    // Test serialization without field IDs (existing behavior)
    String jsonWithoutFieldIds = ExpressionParser.toJson(boundSimple, true);
    assertThat(jsonWithoutFieldIds).contains("\"term\" : \"id\"");
    assertThat(jsonWithoutFieldIds).doesNotContain("fieldId");

    // Test serialization with field IDs (new behavior)
    String jsonWithFieldIds = ExpressionParser.toResolvedJson(boundSimple, true);
    assertThat(jsonWithFieldIds).contains("\"type\" : \"reference\"");
    assertThat(jsonWithFieldIds).contains("\"term\" : \"id\"");
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 100");

    // Test complex expressions
    Expression complexExpr =
        Expressions.and(
            Expressions.equal(Expressions.ref("data", 101), "test"),
            Expressions.greaterThan(Expressions.ref("id", 100), 50L));
    Expression boundComplex = Binder.bind(STRUCT_TYPE, complexExpr, true);

    String complexJsonWithFieldIds = ExpressionParser.toResolvedJson(boundComplex, true);
    assertThat(complexJsonWithFieldIds).contains("\"source-id\" : 100");
    assertThat(complexJsonWithFieldIds).contains("\"source-id\" : 101");
    assertThat(complexJsonWithFieldIds).contains("\"term\" : \"id\"");
    assertThat(complexJsonWithFieldIds).contains("\"term\" : \"data\"");
  }

  @Test
  public void testBoundTransformExpressionSerializationWithIDReference() {
    // Test that bound transform expressions serialize to JSON with IDReference terms when
    // includeFieldIds=true

    // Create transform expressions using ResolvedTransform
    Expression bucketExpr = Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3);
    Expression dayExpr =
        Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15");

    // Bind the expressions
    Expression boundBucket = Binder.bind(STRUCT_TYPE, bucketExpr, true);
    Expression boundDay = Binder.bind(STRUCT_TYPE, dayExpr, true);

    // Test serialization without field IDs (existing behavior)
    String bucketJsonNoFieldIds = ExpressionParser.toJson(boundBucket, true);
    assertThat(bucketJsonNoFieldIds).contains("\"transform\" : \"bucket[8]\"");
    assertThat(bucketJsonNoFieldIds).contains("\"term\" : \"id\"");
    assertThat(bucketJsonNoFieldIds).doesNotContain("fieldId");

    // Test serialization with field IDs (new behavior)
    String bucketJsonWithFieldIds = ExpressionParser.toResolvedJson(boundBucket, true);
    assertThat(bucketJsonWithFieldIds).contains("\"transform\" : \"bucket[8]\"");
    assertThat(bucketJsonWithFieldIds).contains("\"type\" : \"reference\"");
    assertThat(bucketJsonWithFieldIds).contains("\"term\" : \"id\"");
    assertThat(bucketJsonWithFieldIds).contains("\"source-id\" : 100");

    String dayJsonWithFieldIds = ExpressionParser.toResolvedJson(boundDay, true);
    assertThat(dayJsonWithFieldIds).contains("\"transform\" : \"day\"");
    assertThat(dayJsonWithFieldIds).contains("\"type\" : \"reference\"");
    assertThat(dayJsonWithFieldIds).contains("\"term\" : \"date\"");
    assertThat(dayJsonWithFieldIds).contains("\"source-id\" : 107");
  }

  @Test
  public void testComplexBoundTransformExpressionSerializationWithResolvedReference() {
    // Test complex bound expressions with mixed transforms and references
    Expression complexExpr =
        Expressions.and(
            Expressions.or(
                Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
                Expressions.equal(Expressions.day(Expressions.ref("date", 107)), "2023-01-15")),
            Expressions.and(
                Expressions.equal(Expressions.truncate(Expressions.ref("data", 101), 4), "test"),
                Expressions.isNull(Expressions.ref("f", 105))));

    // Bind the complex expression
    Expression bound = Binder.bind(STRUCT_TYPE, complexExpr, true);

    // Test serialization with field IDs
    String jsonWithFieldIds = ExpressionParser.toResolvedJson(bound, true);

    // Verify all field IDs are present
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 100"); // id field
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 107"); // date field
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 101"); // data field
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 105"); // f field

    // Verify all field names are present
    assertThat(jsonWithFieldIds).contains("\"term\" : \"id\"");
    assertThat(jsonWithFieldIds).contains("\"term\" : \"date\"");
    assertThat(jsonWithFieldIds).contains("\"term\" : \"data\"");
    assertThat(jsonWithFieldIds).contains("\"term\" : \"f\"");

    // Verify transforms are serialized correctly
    assertThat(jsonWithFieldIds).contains("\"transform\" : \"bucket[8]\"");
    assertThat(jsonWithFieldIds).contains("\"transform\" : \"day\"");
    assertThat(jsonWithFieldIds).contains("\"transform\" : \"truncate[4]\"");

    // Verify IDReference structure for both transforms and direct references
    // Count occurrences of "type" : "reference" to ensure all references use IDReference
    // format
    long refTypeCount =
        jsonWithFieldIds
            .lines()
            .mapToLong(
                line -> {
                  int count = 0;
                  int index = 0;
                  String pattern = "\"type\" : \"reference\"";
                  while ((index = line.indexOf(pattern, index)) != -1) {
                    count++;
                    index += pattern.length();
                  }
                  return count;
                })
            .sum();

    assertThat(refTypeCount).isEqualTo(4); // Should have 4 IDReference objects
  }

  @Test
  public void testBoundExpressionRoundTripWithIDReference() {
    // Test that bound expressions with IDReference can be serialized and parsed back

    // Create expressions using both ResolvedTransform and IDReference
    Expression originalExpr =
        Expressions.and(
            Expressions.equal(Expressions.bucket(Expressions.ref("id", 100), 8), 3),
            Expressions.equal(Expressions.ref("data", 101), "test"));

    // Bind the expression
    Expression bound = Binder.bind(STRUCT_TYPE, originalExpr, true);

    // Serialize with field IDs
    String jsonWithFieldIds = ExpressionParser.toResolvedJson(bound, true);

    // Verify the JSON structure contains complete IDReference information
    assertThat(jsonWithFieldIds).contains("\"type\" : \"reference\"");
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 100");
    assertThat(jsonWithFieldIds).contains("\"source-id\" : 101");

    // Parse back from JSON with field IDs
    Expression parsed = ExpressionParser.fromJson(jsonWithFieldIds, SCHEMA);

    // Verify parsed expression uses IDReference
    assertThat(parsed).isInstanceOf(And.class);
    And parsedAnd = (And) parsed;

    UnboundPredicate<?> leftPred = (UnboundPredicate<?>) parsedAnd.left();
    assertThat(leftPred.term()).isInstanceOf(UnboundTransform.class);
    UnboundTransform<?, ?> leftTransform = (UnboundTransform<?, ?>) leftPred.term();
    assertThat(leftTransform.ref()).isInstanceOf(IDReference.class);
    assertThat(((IDReference<?>) leftTransform.ref()).id()).isEqualTo(100);

    UnboundPredicate<?> rightPred = (UnboundPredicate<?>) parsedAnd.right();
    assertThat(rightPred.term()).isInstanceOf(IDReference.class);
    IDReference<?> rightRef = (IDReference<?>) rightPred.term();
    assertThat(rightRef.id()).isEqualTo(101);
    assertThat(rightRef.name()).isEqualTo("data");

    // Verify that binding the parsed expression produces the same result
    Expression parsedBound = Binder.bind(STRUCT_TYPE, parsed, true);
    assertThat(parsedBound.toString()).isEqualTo(bound.toString());
  }

  @Test
  public void testIDReferenceRoundTripWithFieldIds() {
    // Test that IDReference expressions with field IDs can be serialized and parsed back
    Expression original = Expressions.equal(Expressions.ref("id", 100), 42L);

    // Serialize without binding (preserves IDReference)
    String json = ExpressionParser.toJson(original, true);

    // For unbound expressions, field IDs are not included in JSON by default
    // We need to manually create JSON with field IDs to test parsing
    String jsonWithFieldIds =
        "{\n"
            + "  \"type\" : \"eq\",\n"
            + "  \"term\" : {\n"
            + "    \"type\" : \"reference\",\n"
            + "    \"term\" : \"id\",\n"
            + "    \"source-id\" : 100\n"
            + "  },\n"
            + "  \"value\" : 42\n"
            + "}";

    // Parse the JSON with field IDs
    Expression parsed = ExpressionParser.fromJson(jsonWithFieldIds, SCHEMA);

    // Verify it creates a IDReference
    assertThat(parsed).isInstanceOf(UnboundPredicate.class);
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) parsed;
    assertThat(predicate.term()).isInstanceOf(IDReference.class);

    IDReference<?> resolvedRef = (IDReference<?>) predicate.term();
    assertThat(resolvedRef.name()).isEqualTo("id");
    assertThat(resolvedRef.id()).isEqualTo(100);

    // Verify equivalence after binding
    Expression originalBound = Binder.bind(STRUCT_TYPE, original, true);
    Expression parsedBound = Binder.bind(STRUCT_TYPE, parsed, true);
    assertThat(parsedBound.toString()).isEqualTo(originalBound.toString());
  }

  @Test
  public void testBoundExpressionIdPreservationAcrossAllTypes() {
    // Test that all data types preserve field IDs correctly in bound expression serialization

    // Create expressions for fields that support meaningful comparisons
    Expression[] expressions =
        new Expression[] {
          Expressions.equal(Expressions.ref("id", 100), 42L),
          Expressions.equal(Expressions.ref("data", 101), "test"),
          Expressions.equal(Expressions.ref("b", 102), true),
          Expressions.equal(Expressions.ref("i", 103), 42),
          Expressions.equal(Expressions.ref("l", 104), 42L),
          Expressions.equal(Expressions.ref("f", 105), 3.14f),
          Expressions.equal(Expressions.ref("d", 106), 3.14159),
          Expressions.equal(Expressions.ref("s", 110), "test-string"),
          Expressions.greaterThan(Expressions.ref("id", 100), 10L)
        };

    for (Expression expr : expressions) {
      // Bind each expression
      Expression bound = Binder.bind(STRUCT_TYPE, expr, true);

      // Serialize with field IDs
      String json = ExpressionParser.toResolvedJson(bound, true);

      // Extract expected field ID from the original IDReference
      UnboundPredicate<?> unboundPred = (UnboundPredicate<?>) expr;
      IDReference<?> resolvedRef = (IDReference<?>) unboundPred.term();
      int expectedFieldId = resolvedRef.id();

      // Verify the field ID is preserved in the JSON
      assertThat(json).contains("\"source-id\" : " + expectedFieldId);
      assertThat(json).contains("\"type\" : \"reference\"");
    }
  }
}
