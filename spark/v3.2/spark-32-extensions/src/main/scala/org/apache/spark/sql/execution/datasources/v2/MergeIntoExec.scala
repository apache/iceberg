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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BasePredicate
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoParams
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

case class MergeIntoExec(
    mergeIntoParams: MergeIntoParams,
    output: Seq[Attribute],
    override val child: SparkPlan) extends UnaryExecNode {

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions {
      processPartition(mergeIntoParams, _)
    }
  }

  private def generateProjection(exprs: Seq[Expression], attrs: Seq[Attribute]): UnsafeProjection = {
    UnsafeProjection.create(exprs, attrs)
  }

  private def generatePredicate(expr: Expression, attrs: Seq[Attribute]): BasePredicate = {
    GeneratePredicate.generate(expr, attrs)
  }

  private def applyProjection(
      actions: Seq[(BasePredicate, Option[UnsafeProjection])],
      inputRow: InternalRow): InternalRow = {

    // Find the first combination where the predicate evaluates to true.
    // In case when there are overlapping condition in the MATCHED
    // clauses, for the first one that satisfies the predicate, the
    // corresponding action is applied. For example:
    //   WHEN MATCHED AND id > 1 AND id < 10 UPDATE *
    //   WHEN MATCHED AND id = 5 OR id = 21 DELETE
    // In above case, when id = 5, it applies both that matched predicates. In this
    // case the first one we see is applied.

    val pair = actions.find {
      case (predicate, _) => predicate.eval(inputRow)
    }

    // Now apply the appropriate projection to produce an output row, or return null to suppress this row
    pair match {
      case Some((_, Some(projection))) =>
        projection.apply(inputRow)
      case _ =>
        null
    }
  }

  private def processPartition(
     params: MergeIntoParams,
     rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {

    val joinedAttrs = params.joinedAttributes
    val isSourceRowPresentPred = generatePredicate(params.isSourceRowPresent, joinedAttrs)
    val isTargetRowPresentPred = generatePredicate(params.isTargetRowPresent, joinedAttrs)
    val matchedPreds = params.matchedConditions.map(generatePredicate(_, joinedAttrs))
    val matchedProjs = params.matchedOutputs.map(_.map(generateProjection(_, joinedAttrs)))
    val notMatchedPreds = params.notMatchedConditions.map(generatePredicate(_, joinedAttrs))
    val notMatchedProjs = params.notMatchedOutputs.map(_.map(generateProjection(_, joinedAttrs)))
    val projectTargetCols = generateProjection(params.targetOutput, joinedAttrs)
    val nonMatchedPairs =   notMatchedPreds zip notMatchedProjs
    val matchedPairs = matchedPreds zip matchedProjs

    /**
     * This method is responsible for processing a input row to emit the resultant row with an
     * additional column that indicates whether the row is going to be included in the final
     * output of merge or not.
     * 1. If there is a target row for which there is no corresponding source row (join condition not met)
     *    - Only project the target columns with deleted flag set to false.
     * 2. If there is a source row for which there is no corresponding target row (join condition not met)
     *    - Apply the not matched actions (i.e INSERT actions) if non match conditions are met.
     * 3. If there is a source row for which there is a corresponding target row (join condition met)
     *    - Apply the matched actions (i.e DELETE or UPDATE actions) if match conditions are met.
     */
    def processRow(inputRow: InternalRow): InternalRow = {
      if (!isSourceRowPresentPred.eval(inputRow)) {
        projectTargetCols.apply(inputRow)
      } else if (!isTargetRowPresentPred.eval(inputRow)) {
        applyProjection(nonMatchedPairs, inputRow)
      } else {
        applyProjection(matchedPairs, inputRow)
      }
    }

    rowIterator
      .map(processRow)
      .filter(row => row != null)
  }
}
