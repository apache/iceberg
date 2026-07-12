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

import org.apache.iceberg.geospatial.BoundingBox;

/**
 * A bound predicate that matches when a geometry/geography value intersects a constant query
 * bounding box. Unlike literal predicates, the constant here is a {@link BoundingBox} (a spatial
 * window) rather than a scalar {@link Literal}, so it is modeled as its own predicate kind.
 */
public class BoundSpatialPredicate extends BoundPredicate<Object> {
  private final BoundingBox queryBound;

  BoundSpatialPredicate(Operation op, BoundTerm<Object> term, BoundingBox queryBound) {
    super(op, term);
    this.queryBound = queryBound;
  }

  public BoundingBox queryBound() {
    return queryBound;
  }

  @Override
  public boolean isSpatialPredicate() {
    return true;
  }

  @Override
  public BoundSpatialPredicate asSpatialPredicate() {
    return this;
  }

  @Override
  public boolean test(Object value) {
    // Row-level evaluation is not implemented for this PoC; spatial predicates are used for
    // file-level metrics pruning (see InclusiveMetricsEvaluator).
    throw new UnsupportedOperationException("Row-level spatial evaluation is not implemented");
  }

  @Override
  public String toString() {
    switch (op()) {
      case ST_INTERSECTS:
        return "st_intersects(" + term() + ", " + queryBound + ")";
      default:
        return "Invalid spatial predicate: operation = " + op();
    }
  }
}
