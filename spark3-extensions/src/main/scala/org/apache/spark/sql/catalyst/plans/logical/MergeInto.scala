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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.col

case class MergeInto(mergeIntoProcessor: MergeIntoProcessor,
                     targetRelation: DataSourceV2Relation,
                     child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = targetRelation.output
}

class MergeIntoProcessor(isSourceRowNotPresent: Expression,
                         isTargetRowNotPresent: Expression,
                         matchedConditions: Seq[Expression],
                         matchedOutputs: Seq[Seq[Expression]],
                         notMatchedConditions: Seq[Expression],
                         notMatchedOutputs: Seq[Seq[Expression]],
                         targetOutput: Seq[Expression],
                         joinedAttributes: Seq[Attribute]) extends Serializable {

  private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, joinedAttributes)
  }

  private def generatePredicate(expr: Expression): BasePredicate = {
    GeneratePredicate.generate(expr, joinedAttributes)
  }

  def processPartition(rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val isSourceRowNotPresentPred = generatePredicate(isSourceRowNotPresent)
    val isTargetRowNotPresentPred = generatePredicate(isTargetRowNotPresent)
    val matchedPreds = matchedConditions.map(generatePredicate)
    val matchedProjs = matchedOutputs.map(generateProjection)
    val notMatchedPreds = notMatchedConditions.map(generatePredicate)
    val notMatchedProjs = notMatchedOutputs.map(generateProjection)
    val projectTargetCols = generateProjection(targetOutput)

    def shouldDeleteRow(row: InternalRow): Boolean =
      row.getBoolean(targetOutput.size - 1)

    def applyProjection(predicates: Seq[BasePredicate],
                        projections: Seq[UnsafeProjection],
                        inputRow: InternalRow): InternalRow = {
      // Find the first combination where the predicate evaluates to true
      val pair = (predicates zip projections).find {
        case (predicate, _) => predicate.eval(inputRow)
      }

      // Now apply the appropriate projection to either :
      // - Insert a row into target
      // - Update a row of target
      // - Delete a row in target. The projected row will have the deleted bit set.
      pair match {
        case Some((_, projection)) =>
          projection.apply(inputRow)
        case None =>
          projectTargetCols.apply(inputRow)
      }
    }

    def processRow(inputRow: InternalRow): InternalRow = {
      isSourceRowNotPresentPred.eval(inputRow) match {
        case true => projectTargetCols.apply(inputRow)
        case false =>
          if (isTargetRowNotPresentPred.eval(inputRow)) {
            applyProjection(notMatchedPreds, notMatchedProjs, inputRow)
          } else {
            applyProjection(matchedPreds, matchedProjs, inputRow)
          }
      }
    }

    rowIterator
      .map(processRow)
      .filter(!shouldDeleteRow(_))
  }
}
