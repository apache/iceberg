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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.iceberg.spark.functions.SparkFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.AbstractDataType
import org.apache.spark.sql.types.DataType

/**
 * Spark analyzes the Iceberg system function to {@link StaticInvoke} which could not be push
 * down to datasource. This rule will replace {@link StaticInvoke} to
 * {@link ApplyFunctionExpression} for Iceberg system function in a filter condition.
 */
case class RewriteStaticInvoke(spark: SparkSession) extends Rule[LogicalPlan] {

  private def systemFuncPushDownEnabled(): Boolean =
    spark.conf.get(
      SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED,
      SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED_DEFAULT.toString
    ).toBoolean

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case f @ Filter(condition, _) if systemFuncPushDownEnabled() && f.resolved =>
      val newCondition = condition.transform {
        case invoke @ StaticInvoke(clazz, _, functionName, arguments, inputTypes, _, _, _)
          if "invoke" == functionName =>
          toDataTypes(inputTypes) match {
            case Some(inputDataTypes) =>
              val systemFunc = SparkFunctions.buildFromClass(clazz, inputDataTypes)
              if (systemFunc == null) {
                invoke
              } else {
                ApplyFunctionExpression(systemFunc, arguments)
              }

            case _ => invoke
          }

        case other => other
      }

      if (newCondition fastEquals condition) {
        f
      } else {
        f.copy(condition = newCondition)
      }
  }

  private def toDataTypes(inputTypes: Seq[AbstractDataType]): Option[Array[DataType]] = {
    if (inputTypes.forall(_.isInstanceOf[DataType])) {
      Some(inputTypes.map(_.asInstanceOf[DataType]).toArray)
    } else {
      None
    }
  }
}
