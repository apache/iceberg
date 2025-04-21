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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBoundGeospatialPredicate {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "point", Types.GeometryType.crs84()),
          required(2, "geography", Types.GeographyType.crs84()),
          required(3, "point2", Types.GeometryType.crs84()),
          required(4, "geography2", Types.GeographyType.crs84()));

  private static Stream<Arguments> geospatialOperators() {
    return Stream.of(
        Arguments.of(Expression.Operation.ST_INTERSECTS, "point", 1),
        Arguments.of(Expression.Operation.ST_DISJOINT, "geography", 2));
  }

  @ParameterizedTest
  @MethodSource("geospatialOperators")
  public void testGeospatialPredicateBinding(
      Expression.Operation op, String fieldName, int fieldId) {
    // Create a bounding box for testing
    GeospatialBound min = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox bbox = new BoundingBox(min, max);

    // Create an unbound predicate based on the operation
    UnboundPredicate<ByteBuffer> unbound = Expressions.geospatialPredicate(op, fieldName, bbox);

    // Bind the predicate to the schema
    Expression bound = unbound.bind(SCHEMA.asStruct());

    // Verify the bound predicate is a BoundGeospatialPredicate
    assertThat(bound).isInstanceOf(BoundGeospatialPredicate.class);
    BoundGeospatialPredicate predicate = (BoundGeospatialPredicate) bound;

    // Verify the operation matches the expected operation
    assertThat(predicate.op()).isEqualTo(op);

    // Verify the term references the correct field
    assertThat(predicate.term().ref().fieldId()).isEqualTo(fieldId);

    // Verify the literal value is correct
    assertThat(predicate.literal().value()).isEqualTo(bbox);

    // Verify the predicate is identified as a geospatial predicate
    assertThat(predicate.isGeospatialPredicate()).isTrue();

    // Only check asGeospatialPredicate for ST_INTERSECTS to maintain original test behavior
    if (op == Expression.Operation.ST_INTERSECTS) {
      assertThat(predicate.asGeospatialPredicate()).isSameAs(predicate);
    }
  }

  @ParameterizedTest
  @MethodSource("geospatialOperators")
  public void testNegation(Expression.Operation op, String fieldName, int fieldId) {
    // Create a bounding box for testing
    GeospatialBound min = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox bbox = new BoundingBox(min, max);

    // Create an unbound predicate based on the operation
    UnboundPredicate<ByteBuffer> unbound = Expressions.geospatialPredicate(op, fieldName, bbox);

    // Bind the predicate to the schema
    Expression bound = unbound.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate = (BoundGeospatialPredicate) bound;

    // Negate the predicate
    Expression negated = predicate.negate();

    // Verify the negated predicate is a BoundGeospatialPredicate
    assertThat(negated).isInstanceOf(BoundGeospatialPredicate.class);
    BoundGeospatialPredicate negatedPredicate = (BoundGeospatialPredicate) negated;

    // Verify the operation is the opposite of the original
    Expression.Operation expectedNegatedOp =
        (op == Expression.Operation.ST_INTERSECTS)
            ? Expression.Operation.ST_DISJOINT
            : Expression.Operation.ST_INTERSECTS;
    assertThat(negatedPredicate.op()).isEqualTo(expectedNegatedOp);

    // Verify the term and literal are unchanged
    assertThat(negatedPredicate.term()).isEqualTo(predicate.term());
    assertThat(negatedPredicate.literal().value()).isEqualTo(predicate.literal().value());

    // Test double negation
    Expression doubleNegated = negatedPredicate.negate();
    assertThat(doubleNegated).isInstanceOf(BoundGeospatialPredicate.class);
    BoundGeospatialPredicate doubleNegatedPredicate = (BoundGeospatialPredicate) doubleNegated;

    // Verify the operation is back to the original
    assertThat(doubleNegatedPredicate.op()).isEqualTo(op);
  }

  @ParameterizedTest
  @MethodSource("geospatialOperators")
  public void testEquivalence(Expression.Operation op, String fieldName, int fieldId) {
    // Create two identical bounding boxes
    GeospatialBound min1 = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max1 = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox bbox1 = new BoundingBox(min1, max1);
    GeospatialBound min2 = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox bbox2 = new BoundingBox(min2, max2);

    // Create a different bounding box
    GeospatialBound min3 = GeospatialBound.createXY(5.0, 6.0);
    GeospatialBound max3 = GeospatialBound.createXY(7.0, 8.0);
    BoundingBox bbox3 = new BoundingBox(min3, max3);

    // Create the main predicate with the current operation
    UnboundPredicate<ByteBuffer> unbound1 = Expressions.geospatialPredicate(op, fieldName, bbox1);
    Expression bound1 = unbound1.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate1 = (BoundGeospatialPredicate) bound1;

    // Create a predicate with the same operation and same bounding box
    UnboundPredicate<ByteBuffer> unbound2 = Expressions.geospatialPredicate(op, fieldName, bbox2);
    Expression bound2 = unbound2.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate2 = (BoundGeospatialPredicate) bound2;

    // Create a predicate with the same operation but different bounding box
    UnboundPredicate<ByteBuffer> unbound3 = Expressions.geospatialPredicate(op, fieldName, bbox3);
    Expression bound3 = unbound3.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate3 = (BoundGeospatialPredicate) bound3;

    // Create a predicate with the opposite operation and same bounding box
    UnboundPredicate<ByteBuffer> unbound4 =
        Expressions.geospatialPredicate(op.negate(), fieldName, bbox1);
    Expression bound4 = unbound4.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate4 = (BoundGeospatialPredicate) bound4;

    // Create a predicate with the same operation and the same bounding box, but different field
    // name
    UnboundPredicate<ByteBuffer> unbound5 = Expressions.geospatialPredicate(op, "point2", bbox1);
    Expression bound5 = unbound5.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate5 = (BoundGeospatialPredicate) bound5;

    UnboundPredicate<ByteBuffer> unbound6 =
        Expressions.geospatialPredicate(op, "geography2", bbox1);
    Expression bound6 = unbound6.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate6 = (BoundGeospatialPredicate) bound6;

    // Test equivalence
    assertThat(predicate1.isEquivalentTo(predicate2)).isTrue();
    assertThat(predicate2.isEquivalentTo(predicate1)).isTrue();

    // Different bounding box
    assertThat(predicate1.isEquivalentTo(predicate3)).isFalse();

    // Different operation
    assertThat(predicate1.isEquivalentTo(predicate4)).isFalse();

    // Different field name
    assertThat(predicate1.isEquivalentTo(predicate5)).isFalse();
    assertThat(predicate1.isEquivalentTo(predicate6)).isFalse();

    // Not a geospatial predicate
    assertThat(predicate1.isEquivalentTo(Expressions.alwaysTrue())).isFalse();
  }

  @ParameterizedTest
  @MethodSource("geospatialOperators")
  public void testToString(Expression.Operation op, String fieldName, int fieldId) {
    // Create a bounding box for testing
    GeospatialBound min = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox bbox = new BoundingBox(min, max);

    // Create an unbound predicate based on the operation
    UnboundPredicate<ByteBuffer> unbound = Expressions.geospatialPredicate(op, fieldName, bbox);

    // Bind the predicate to the schema
    Expression bound = unbound.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate = (BoundGeospatialPredicate) bound;

    // Verify toString output contains the operation name
    String expectedOpString =
        (op == Expression.Operation.ST_INTERSECTS) ? "stIntersects" : "stDisjoint";
    assertThat(predicate.toString()).contains(expectedOpString);

    // Verify toString output contains the field ID
    assertThat(predicate.toString()).contains("id=" + fieldId);
  }

  @ParameterizedTest
  @MethodSource("geospatialOperators")
  public void testWithComplexBoundingBox(Expression.Operation op, String fieldName, int fieldId) {
    // Create a bounding box with Z and M coordinates
    GeospatialBound min = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 4.0);
    GeospatialBound max = GeospatialBound.createXYZM(5.0, 6.0, 7.0, 8.0);
    BoundingBox bbox = new BoundingBox(min, max);

    // Create an unbound predicate based on the operation
    UnboundPredicate<ByteBuffer> unbound = Expressions.geospatialPredicate(op, fieldName, bbox);

    // Bind the predicate to the schema
    Expression bound = unbound.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate = (BoundGeospatialPredicate) bound;

    // Verify the operation matches the expected operation
    assertThat(predicate.op()).isEqualTo(op);

    // Verify the term references the correct field
    assertThat(predicate.term().ref().fieldId()).isEqualTo(fieldId);

    // Verify the literal value is correct
    assertThat(predicate.literal().value()).isEqualTo(bbox);

    // Verify Z and M coordinates are preserved
    BoundingBox boundingBox = predicate.literal().value();
    assertThat(boundingBox.min().hasZ()).isTrue();
    assertThat(boundingBox.min().hasM()).isTrue();
    assertThat(boundingBox.min().z()).isEqualTo(3.0);
    assertThat(boundingBox.min().m()).isEqualTo(4.0);
    assertThat(boundingBox.max().hasZ()).isTrue();
    assertThat(boundingBox.max().hasM()).isTrue();
    assertThat(boundingBox.max().z()).isEqualTo(7.0);
    assertThat(boundingBox.max().m()).isEqualTo(8.0);
  }

  @ParameterizedTest
  @MethodSource("geospatialOperators")
  public void testWithSpecialValues(Expression.Operation op, String fieldName, int fieldId) {
    // Create a bounding box with NaN and infinity values
    GeospatialBound min = GeospatialBound.createXY(Double.NEGATIVE_INFINITY, Double.NaN);
    GeospatialBound max = GeospatialBound.createXY(Double.POSITIVE_INFINITY, Double.NaN);
    BoundingBox bbox = new BoundingBox(min, max);

    // Create an unbound predicate based on the operation
    UnboundPredicate<ByteBuffer> unbound = Expressions.geospatialPredicate(op, fieldName, bbox);

    // Bind the predicate to the schema
    Expression bound = unbound.bind(SCHEMA.asStruct());
    BoundGeospatialPredicate predicate = (BoundGeospatialPredicate) bound;

    // Verify the operation matches the expected operation
    assertThat(predicate.op()).isEqualTo(op);

    // Verify the term references the correct field
    assertThat(predicate.term().ref().fieldId()).isEqualTo(fieldId);

    // Verify the literal value is correct
    assertThat(predicate.literal().value()).isEqualTo(bbox);

    // Verify special values are preserved
    BoundingBox boundingBox = predicate.literal().value();
    assertThat(boundingBox.min().x()).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(Double.isNaN(boundingBox.min().y())).isTrue();
    assertThat(boundingBox.max().x()).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(Double.isNaN(boundingBox.max().y())).isTrue();
  }
}
