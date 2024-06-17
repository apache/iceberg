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

/**
 * The aggregate functions that can be pushed and evaluated in Iceberg. Currently only three
 * aggregate functions Max, Min and Count are supported.
 */
public abstract class Aggregate<C extends Term> implements Expression {
  private final Operation op;
  private final C term;

  Aggregate(Operation op, C term) {
    this.op = op;
    this.term = term;
  }

  @Override
  public Operation op() {
    return op;
  }

  public C term() {
    return term;
  }

  @Override
  public String toString() {
    switch (op()) {
      case COUNT:
        return "count(" + term() + ")";
      case COUNT_STAR:
        return "count(*)";
      case COUNT_DISTINCT:
        return "count(distinct" + term() + ")";
      case MAX:
        return "max(" + term() + ")";
      case MIN:
        return "min(" + term() + ")";
      default:
        throw new UnsupportedOperationException("Invalid aggregate: " + op());
    }
  }
}
