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
package org.apache.iceberg.spark.extensions

import org.apache.iceberg.spark.udf.IcebergSqlUdfScalarFunction
import org.apache.iceberg.spark.udf.SqlFunctionSpecParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import scala.jdk.CollectionConverters._

/**
 * Post-hoc resolution rule that rewrites Iceberg SQL scalar UDF *calls* into native Catalyst
 * expressions.
 *
 * <p>Spark resolves a V2 catalog function call by binding an {@code UnboundFunction} into a {@code
 * ScalarFunction} and then representing the invocation as an {@link ApplyFunctionExpression}. For
 * Iceberg SQL UDFs, the catalog returns a marker {@code ScalarFunction} ({@code
 * IcebergSqlUdfScalarFunction}) that carries the SQL body and parameter/return type metadata.
 *
 * <p>This rule matches {@code ApplyFunctionExpression(IcebergSqlUdfScalarFunction, args)} and
 * replaces it with:
 *
 * <ul>
 *   <li>a Catalyst expression parsed from {@code udf.body()}
 *   <li>positional parameter binding via {@code Cast(arg, paramType)}
 *   <li>a final {@code Cast(..., returnType)}
 * </ul>
 *
 * <p>Stage 1 scope: scalar-only, Spark dialect-only, positional arguments only (no named/defaults),
 * no overload resolution beyond what the catalog returns.
 *
 * <p>This avoids session-scoped temporary function registration and keeps UDF semantics in the
 * analyzed plan.
 */
class RewriteSqlFunctions(spark: SparkSession) extends Rule[LogicalPlan] {

  private def substituteParams(expr: Expression, bindings: Map[String, Expression]): Expression = {
    expr.transform { case a: UnresolvedAttribute =>
      val name = a.nameParts.mkString(".")
      bindings.getOrElse(name, a)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case ApplyFunctionExpression(func, args) if func.isInstanceOf[IcebergSqlUdfScalarFunction] =>
        val udf = func.asInstanceOf[IcebergSqlUdfScalarFunction]
        val parsed = spark.sessionState.sqlParser.parseExpression(udf.body())

        val paramNames = udf.paramNames().asScala.toSeq
        val paramTypes = udf.paramIcebergTypeJson().asScala.toSeq
        if (args.length != paramNames.length) {
          throw new IllegalArgumentException(
            s"Function ${udf.name()} expects ${paramNames.length} arguments but got ${args.length}")
        }

        val bindings: Map[String, Expression] =
          paramNames
            .zip(paramTypes)
            .zip(args)
            .map { case ((n, t), a) =>
              val sparkType = SqlFunctionSpecParser.icebergTypeToSparkType(t)
              (n, Cast(a, sparkType))
            }
            .toMap

        val substituted = substituteParams(parsed, bindings)
        val returnType = SqlFunctionSpecParser.icebergTypeToSparkType(udf.returnIcebergTypeJson())
        Cast(substituted, returnType)
    }
  }
}
