/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.transforms;

import com.google.common.collect.Lists;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Projections;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

import static com.netflix.iceberg.TestHelpers.assertAndUnwrap;
import static com.netflix.iceberg.TestHelpers.assertAndUnwrapUnbound;

public class TestProjection {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(16, "id", Types.LongType.get())
  );

  @Test
  public void testIdentityProjection() {
    List<UnboundPredicate<?>> predicates = Lists.newArrayList(
        Expressions.notNull("id"),
        Expressions.isNull("id"),
        Expressions.lessThan("id", 100),
        Expressions.lessThanOrEqual("id", 101),
        Expressions.greaterThan("id", 102),
        Expressions.greaterThanOrEqual("id", 103),
        Expressions.equal("id", 104),
        Expressions.notEqual("id", 105)
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("id")
        .build();

    for (UnboundPredicate<?> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.inclusive(spec).project(predicate);
      UnboundPredicate<?> projected = assertAndUnwrapUnbound(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundPredicate<?> bound = assertAndUnwrap(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
          "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      if (bound.literal() != null) {
        Assert.assertEquals("Literal should be equal",
            bound.literal().value(), projected.literal().value());
      } else {
        Assert.assertNull("Literal should be null", projected.literal());
      }
    }
  }

  @Test
  public void testStrictIdentityProjection() {
    List<UnboundPredicate<?>> predicates = Lists.newArrayList(
        Expressions.notNull("id"),
        Expressions.isNull("id"),
        Expressions.lessThan("id", 100),
        Expressions.lessThanOrEqual("id", 101),
        Expressions.greaterThan("id", 102),
        Expressions.greaterThanOrEqual("id", 103),
        Expressions.equal("id", 104),
        Expressions.notEqual("id", 105)
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("id")
        .build();

    for (UnboundPredicate<?> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.strict(spec).project(predicate);
      UnboundPredicate<?> projected = assertAndUnwrapUnbound(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundPredicate<?> bound = assertAndUnwrap(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
          "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      if (bound.literal() != null) {
        Assert.assertEquals("Literal should be equal",
            bound.literal().value(), projected.literal().value());
      } else {
        Assert.assertNull("Literal should be null", projected.literal());
      }
    }
  }
}
