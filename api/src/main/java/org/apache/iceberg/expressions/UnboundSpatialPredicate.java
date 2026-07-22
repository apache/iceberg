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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * An unbound predicate that matches when a geometry/geography column intersects a constant query
 * bounding box. Binding resolves the column reference and validates it is a geospatial type.
 */
public class UnboundSpatialPredicate implements Unbound<Object, Expression>, Expression {
  private final Operation op;
  private final NamedReference<Object> term;
  private final BoundingBox queryBound;

  UnboundSpatialPredicate(Operation op, NamedReference<Object> term, BoundingBox queryBound) {
    this.op = op;
    this.term = term;
    this.queryBound = queryBound;
  }

  @Override
  public Operation op() {
    return op;
  }

  @Override
  public NamedReference<Object> ref() {
    return term;
  }

  public BoundingBox queryBound() {
    return queryBound;
  }

  @Override
  public Expression negate() {
    throw new UnsupportedOperationException("Cannot negate a spatial predicate");
  }

  @Override
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    BoundTerm<Object> bound = term.bind(struct, caseSensitive);
    Type type = bound.type();
    Preconditions.checkArgument(
        type.typeId() == Type.TypeID.GEOMETRY || type.typeId() == Type.TypeID.GEOGRAPHY,
        "Cannot create spatial predicate on non-geospatial column: %s (%s)",
        term.name(),
        type);
    ValidationException.check(
        op == Operation.ST_INTERSECTS, "Unsupported spatial operation: %s", op);
    return new BoundSpatialPredicate(op, bound, queryBound);
  }

  @Override
  public String toString() {
    return "st_intersects(" + term + ", " + queryBound + ")";
  }
}
