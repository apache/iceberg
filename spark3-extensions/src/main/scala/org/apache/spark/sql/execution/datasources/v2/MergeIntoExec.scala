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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BasePredicate, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoParams
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class MergeIntoExec(mergeIntoProcessor: MergeIntoParams,
                         @transient targetRelation: DataSourceV2Relation,
                         override val child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = targetRelation.output

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions {
      processPartition(mergeIntoProcessor, _)
    }
  }

  private def generateProjection(exprs: Seq[Expression], attrs: Seq[Attribute]): UnsafeProjection = {
    UnsafeProjection.create(exprs, attrs)
  }

  private def generatePredicate(expr: Expression, attrs: Seq[Attribute]): BasePredicate = {
    GeneratePredicate.generate(expr, attrs)
  }

  def applyProjection(predicates: Seq[BasePredicate],
                      projections: Seq[UnsafeProjection],
                      projectTargetCols: UnsafeProjection,
                      projectDeleteRow: UnsafeProjection,
                      inputRow: InternalRow,
                      targetRowNotPresent: Boolean): InternalRow = {
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
        if (targetRowNotPresent) {
          projectDeleteRow.apply(inputRow)
        } else {
          projectTargetCols.apply(inputRow)
        }
    }
  }

  def processPartition(params: MergeIntoParams,
                       rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val joinedAttrs = params.joinedAttributes
    val isSourceRowNotPresentPred = generatePredicate(params.isSourceRowNotPresent, joinedAttrs)
    val isTargetRowNotPresentPred = generatePredicate(params.isTargetRowNotPresent, joinedAttrs)
    val matchedPreds = params.matchedConditions.map(generatePredicate(_, joinedAttrs))
    val matchedProjs = params.matchedOutputs.map(generateProjection(_, joinedAttrs))
    val notMatchedPreds = params.notMatchedConditions.map(generatePredicate(_, joinedAttrs))
    val notMatchedProjs = params.notMatchedOutputs.map(generateProjection(_, joinedAttrs))
    val projectTargetCols = generateProjection(params.targetOutput, joinedAttrs)
    val projectDeletedRow = generateProjection(params.deleteOutput, joinedAttrs)

    def shouldDeleteRow(row: InternalRow): Boolean =
      row.getBoolean(params.targetOutput.size - 1)


    def processRow(inputRow: InternalRow): InternalRow = {
      isSourceRowNotPresentPred.eval(inputRow) match {
        case true =>
          projectTargetCols.apply(inputRow)
        case false =>
          if (isTargetRowNotPresentPred.eval(inputRow)) {
            applyProjection(notMatchedPreds, notMatchedProjs, projectTargetCols,
              projectDeletedRow, inputRow, true)
          } else {
            applyProjection(matchedPreds, matchedProjs, projectTargetCols,
              projectDeletedRow,inputRow, false)
          }
      }
    }

    rowIterator
      .map(processRow)
      .filter(!shouldDeleteRow(_))
  }
}
