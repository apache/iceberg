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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, InSubquery, Not}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}

object DeleteCheck extends (LogicalPlan => Unit) {

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      case DeleteFromTable(_, Some(condition)) if hasNullAwarePredicateWithinNot(condition) =>
        // this limitation is present since SPARK-25154 fix is not yet available
        // we use Not(EqualsNullSafe(cond, true)) while deciding which records to keep
        // such conditions are rewritten by Spark as an existential join and currently Spark
        // does not handle correctly NOT IN subqueries nested into other expressions
        failAnalysis("Null-aware predicate sub-queries are not currently supported in DELETE")

      case _ => // OK
    }
  }

  private def hasNullAwarePredicateWithinNot(cond: Expression): Boolean = {
    cond.find {
      case Not(expr) if expr.find(_.isInstanceOf[InSubquery]).isDefined => true
      case _ => false
    }.isDefined
  }

  private def failAnalysis(msg: String): Unit = throw new AnalysisException(msg)
}
