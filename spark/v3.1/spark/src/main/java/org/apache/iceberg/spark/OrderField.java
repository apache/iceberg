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
package org.apache.iceberg.spark;

import org.apache.iceberg.NullOrder;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.iceberg.expressions.NullOrdering;
import org.apache.spark.sql.connector.iceberg.expressions.SortDirection;
import org.apache.spark.sql.connector.iceberg.expressions.SortOrder;

class OrderField implements SortOrder {
  static OrderField column(
      String fieldName, org.apache.iceberg.SortDirection direction, NullOrder nullOrder) {
    return new OrderField(Expressions.column(fieldName), toSpark(direction), toSpark(nullOrder));
  }

  static OrderField bucket(
      String fieldName,
      int numBuckets,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return new OrderField(
        Expressions.bucket(numBuckets, fieldName), toSpark(direction), toSpark(nullOrder));
  }

  static OrderField truncate(
      String fieldName,
      int width,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return new OrderField(
        Expressions.apply("truncate", Expressions.column(fieldName), Expressions.literal(width)),
        toSpark(direction),
        toSpark(nullOrder));
  }

  static OrderField year(
      String fieldName, org.apache.iceberg.SortDirection direction, NullOrder nullOrder) {
    return new OrderField(Expressions.years(fieldName), toSpark(direction), toSpark(nullOrder));
  }

  static OrderField month(
      String fieldName, org.apache.iceberg.SortDirection direction, NullOrder nullOrder) {
    return new OrderField(Expressions.months(fieldName), toSpark(direction), toSpark(nullOrder));
  }

  static OrderField day(
      String fieldName, org.apache.iceberg.SortDirection direction, NullOrder nullOrder) {
    return new OrderField(Expressions.days(fieldName), toSpark(direction), toSpark(nullOrder));
  }

  static OrderField hour(
      String fieldName, org.apache.iceberg.SortDirection direction, NullOrder nullOrder) {
    return new OrderField(Expressions.hours(fieldName), toSpark(direction), toSpark(nullOrder));
  }

  private static SortDirection toSpark(org.apache.iceberg.SortDirection direction) {
    return direction == org.apache.iceberg.SortDirection.ASC
        ? SortDirection.ASCENDING
        : SortDirection.DESCENDING;
  }

  private static NullOrdering toSpark(NullOrder nullOrder) {
    return nullOrder == NullOrder.NULLS_FIRST ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST;
  }

  private final Expression expr;
  private final SortDirection direction;
  private final NullOrdering nullOrder;

  private OrderField(Expression expr, SortDirection direction, NullOrdering nullOrder) {
    this.expr = expr;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  @Override
  public Expression expression() {
    return expr;
  }

  @Override
  public SortDirection direction() {
    return direction;
  }

  @Override
  public NullOrdering nullOrdering() {
    return nullOrder;
  }

  @Override
  public String describe() {
    return String.format(
        "%s %s %s",
        expr.describe(),
        direction == SortDirection.ASCENDING ? "ASC" : "DESC",
        nullOrder == NullOrdering.NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST");
  }
}
