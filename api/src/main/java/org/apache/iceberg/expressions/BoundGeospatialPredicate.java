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

import java.nio.ByteBuffer;
import org.apache.iceberg.geospatial.GeospatialBoundingBox;

public class BoundGeospatialPredicate extends BoundPredicate<ByteBuffer> {
  private final Literal<GeospatialBoundingBox> literal;

  BoundGeospatialPredicate(
      Operation op, BoundTerm<ByteBuffer> term, Literal<GeospatialBoundingBox> literal) {
    super(op, term);
    this.literal = literal;
  }

  public Literal<GeospatialBoundingBox> literal() {
    return literal;
  }

  @Override
  public boolean test(ByteBuffer value) {
    return true;
  }

  @Override
  public boolean isGeospatialPredicate() {
    return true;
  }

  @Override
  public BoundGeospatialPredicate asGeospatialPredicate() {
    return this;
  }

  @Override
  public Expression negate() {
    return new BoundGeospatialPredicate(op().negate(), term(), literal);
  }

  @Override
  public boolean isEquivalentTo(Expression expr) {
    if (expr instanceof BoundGeospatialPredicate) {
      BoundGeospatialPredicate other = (BoundGeospatialPredicate) expr;
      return op() == other.op()
          && term().isEquivalentTo(other.term())
          && literal.value().equals(other.literal.value());
    }
    return false;
  }

  @Override
  public String toString() {
    switch (op()) {
      case ST_INTERSECTS:
        return term().toString() + " stIntersects " + literal.value();
      case ST_DISJOINT:
        return term().toString() + " stDisjoint " + literal.value();
      default:
        return "Invalid geospatial predicate: operation = " + op();
    }
  }
}
