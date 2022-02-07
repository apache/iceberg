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
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.internal.SQLConf

case object AlignRowLevelOperations extends Rule[LogicalPlan]
    with AssignmentAlignmentSupport with CastSupport {

  override def conf: SQLConf = SQLConf.get

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UpdateTable if u.resolved && isIcebergRelation(u.table)=>
      u.copy(assignments = alignAssignments(u.table, u.assignments))

    case m: MergeIntoTable if m.resolved && isIcebergRelation(m.targetTable) =>
      val alignedMatchedActions = m.matchedActions.map {
        case u @ UpdateAction(_, assignments) =>
          u.copy(assignments = alignAssignments(m.targetTable, assignments))
        case d: DeleteAction =>
          d
        case _ =>
          throw new AnalysisException("Matched actions can only contain UPDATE or DELETE")
      }

      val alignedNotMatchedActions = m.notMatchedActions.map {
        case i @ InsertAction(_, assignments) =>
          // check no nested columns are present
          val refs = assignments.map(_.key).map(asAssignmentReference)
          refs.foreach { ref =>
            if (ref.size > 1) {
              throw new AnalysisException(
                "Nested fields are not supported inside INSERT clauses of MERGE operations: " +
                s"${ref.mkString("`", "`.`", "`")}")
            }
          }

          val colNames = refs.map(_.head)

          // check there are no duplicates
          val duplicateColNames = colNames.groupBy(identity).collect {
            case (name, matchingNames) if matchingNames.size > 1 => name
          }

          if (duplicateColNames.nonEmpty) {
            throw new AnalysisException(
              s"Duplicate column names inside INSERT clause: ${duplicateColNames.mkString(", ")}")
          }

          // reorder assignments by the target table column order
          val assignmentMap = colNames.zip(assignments).toMap
          i.copy(assignments = alignInsertActionAssignments(m.targetTable, assignmentMap))

        case _ =>
          throw new AnalysisException("Not matched actions can only contain INSERT")
      }

      m.copy(matchedActions = alignedMatchedActions, notMatchedActions = alignedNotMatchedActions)
  }

  private def alignInsertActionAssignments(
      targetTable: LogicalPlan,
      assignmentMap: Map[String, Assignment]): Seq[Assignment] = {

    val resolver = conf.resolver

    targetTable.output.map { targetAttr =>
      val assignment = assignmentMap
        .find { case (name, _) => resolver(name, targetAttr.name) }
        .map { case (_, assignment) => assignment }

      if (assignment.isEmpty) {
        throw new AnalysisException(
          s"Cannot find column '${targetAttr.name}' of the target table among " +
          s"the INSERT columns: ${assignmentMap.keys.mkString(", ")}. " +
          "INSERT clauses must provide values for all columns of the target table.")
      }

      val key = assignment.get.key
      val value = assignment.get.value
      Assignment(key, castIfNeeded(targetAttr, value, resolver))
    }
  }
}
