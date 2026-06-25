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
package org.apache.spark.sql.catalyst.expressions.iceberg

import org.apache.iceberg.expressions.And
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.expressions.False
import org.apache.iceberg.expressions.NamedReference
import org.apache.iceberg.expressions.Not
import org.apache.iceberg.expressions.Or
import org.apache.iceberg.expressions.True
import org.apache.iceberg.expressions.UnboundPredicate
import org.apache.spark.sql.catalyst.expressions.{And => CAnd}
import org.apache.spark.sql.catalyst.expressions.{Expression => CatalystExpression}
import org.apache.spark.sql.catalyst.expressions.{Literal => CLiteral}
import org.apache.spark.sql.catalyst.expressions.{Not => CNot}
import org.apache.spark.sql.catalyst.expressions.{Or => COr}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.IsNaN
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.StartsWith
import org.apache.spark.unsafe.types.UTF8String
import scala.jdk.CollectionConverters._

/**
 * Convert an Iceberg unbound boolean [[Expression]] to a Catalyst expression resolved against
 * the supplied attribute set. Each [[NamedReference]] in the Iceberg expression is matched by
 * name against one of the supplied attributes, preserving that attribute's `ExprId` so
 * downstream references remain resolved.
 *
 * Conversion covers the subset defined by the REST OpenAPI `Expression` schema: True/False,
 * And/Or/Not, unary predicates (is-null/not-null/is-nan/not-nan), literal predicates
 * (eq/not-eq/lt/lt-eq/gt/gt-eq/starts-with/not-starts-with), and set predicates (in/not-in).
 */
object IcebergToSparkExpression {

  def convert(expr: Expression, output: Seq[Attribute]): CatalystExpression = {
    val byName: Map[String, Attribute] = output.map(a => a.name -> a).toMap
    toCatalyst(expr, byName)
  }

  private def toCatalyst(expr: Expression, byName: Map[String, Attribute]): CatalystExpression = {
    expr match {
      case _: True => CLiteral.TrueLiteral
      case _: False => CLiteral.FalseLiteral

      case and: And =>
        CAnd(toCatalyst(and.left, byName), toCatalyst(and.right, byName))
      case or: Or =>
        COr(toCatalyst(or.left, byName), toCatalyst(or.right, byName))
      case not: Not =>
        CNot(toCatalyst(not.child, byName))

      case pred: UnboundPredicate[_] =>
        val attr = resolveRef(pred.ref, byName)
        predicateToCatalyst(pred, attr)

      case other =>
        throw new UnsupportedOperationException(
          s"Cannot convert Iceberg expression to Catalyst: ${other.getClass.getSimpleName}")
    }
  }

  private def predicateToCatalyst(
      pred: UnboundPredicate[_],
      attr: Attribute): CatalystExpression = {
    import Expression.Operation._
    pred.op match {
      case IS_NULL => IsNull(attr)
      case NOT_NULL => IsNotNull(attr)
      case IS_NAN => IsNaN(attr)
      case NOT_NAN => CNot(IsNaN(attr))

      case EQ => EqualTo(attr, literalToCatalyst(pred, attr))
      case NOT_EQ => CNot(EqualTo(attr, literalToCatalyst(pred, attr)))
      case LT => LessThan(attr, literalToCatalyst(pred, attr))
      case LT_EQ => LessThanOrEqual(attr, literalToCatalyst(pred, attr))
      case GT => GreaterThan(attr, literalToCatalyst(pred, attr))
      case GT_EQ => GreaterThanOrEqual(attr, literalToCatalyst(pred, attr))

      case STARTS_WITH => StartsWith(attr, literalToCatalyst(pred, attr))
      case NOT_STARTS_WITH => CNot(StartsWith(attr, literalToCatalyst(pred, attr)))

      case IN =>
        val lits = pred.literals.asScala.map(l => literalValueToCatalyst(l.value, attr)).toSeq
        In(attr, lits)
      case NOT_IN =>
        val lits = pred.literals.asScala.map(l => literalValueToCatalyst(l.value, attr)).toSeq
        CNot(In(attr, lits))

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported predicate operation in row filter: $other")
    }
  }

  private def literalToCatalyst(pred: UnboundPredicate[_], attr: Attribute): CatalystExpression = {
    val literal = pred.literal
    if (literal == null) {
      throw new IllegalArgumentException(s"Predicate has no literal: ${pred.op}")
    }
    literalValueToCatalyst(literal.value, attr)
  }

  private def literalValueToCatalyst(value: Any, attr: Attribute): CatalystExpression = {
    val coerced: Any = value match {
      case s: String => UTF8String.fromString(s)
      case cs: java.lang.CharSequence => UTF8String.fromString(cs.toString)
      case bb: java.nio.ByteBuffer =>
        val arr = new Array[Byte](bb.remaining())
        bb.duplicate().get(arr)
        arr
      case bytes: Array[Byte] => bytes
      case n: java.lang.Number => n
      case b: java.lang.Boolean => b
      case other => other
    }
    CLiteral(coerced, attr.dataType)
  }

  private def resolveRef(ref: NamedReference[_], byName: Map[String, Attribute]): Attribute = {
    byName.getOrElse(
      ref.name,
      throw new IllegalArgumentException(s"Row filter references unknown column: ${ref.name}"))
  }
}
