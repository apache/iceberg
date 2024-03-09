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

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.BasePredicate
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.roaringbitmap.longlong.Roaring64Bitmap

case class MergeRowsExec(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedConditions: Seq[Expression],
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedConditions: Seq[Expression],
    notMatchedOutputs: Seq[Seq[Expression]],
    targetOutput: Seq[Expression],
    performCardinalityCheck: Boolean,
    emitNotMatchedTargetRows: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  private final val ROW_ID = "__row_id"

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient override lazy val references: AttributeSet = child.outputSet

  override def simpleString(maxFields: Int): String = {
    s"MergeRowsExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions(processPartition)
  }

  private def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, child.output)
  }

  private def createPredicate(expr: Expression): BasePredicate = {
    GeneratePredicate.generate(expr, child.output)
  }

  // This method is responsible for processing a input row to emit the resultant row with an
  // additional column that indicates whether the row is going to be included in the final
  // output of merge or not.
  // 1. Found a target row for which there is no corresponding source row (join condition not met)
  //    - Only project the target columns if we need to output unchanged rows (group-based commands)
  // 2. Found a source row for which there is no corresponding target row (join condition not met)
  //    - Apply the not matched actions (i.e INSERT actions) if non match conditions are met.
  // 3. Found a source row for which there is a corresponding target row (join condition met)
  //    - Apply the matched actions (i.e DELETE or UPDATE actions) if match conditions are met.
  private def processPartition(rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val isSourceRowPresentPred = createPredicate(isSourceRowPresent)
    val isTargetRowPresentPred = createPredicate(isTargetRowPresent)

    val matchedActions = matchedConditions.zip(matchedOutputs).map { case (cond, outputs) =>
      outputs match {
        case Seq(output1, output2) =>
          Split(createPredicate(cond), createProjection(output1), createProjection(output2))
        case Seq(output) =>
          Project(createPredicate(cond), createProjection(output))
        case Nil =>
          Project(createPredicate(cond), EmptyProjection)
      }
    }

    val notMatchedActions = notMatchedConditions.zip(notMatchedOutputs).map { case (cond, output) =>
      Project(createPredicate(cond), createProjection(output))
    }

    val projectTargetCols = createProjection(targetOutput)

    val cardinalityCheck = if (performCardinalityCheck) {
      val rowIdOrdinal = child.output.indexWhere(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdOrdinal != -1, "Cannot find row ID attr")
      BitmapCardinalityCheck(rowIdOrdinal)
    } else {
      EmptyCardinalityCheck
    }

    val mergeIterator = if (matchedActions.exists(_.isInstanceOf[Split])) {
      new SplittingMergeRowIterator(
        rowIterator, cardinalityCheck, isTargetRowPresentPred,
        matchedActions, notMatchedActions)
    } else {
      new MergeRowIterator(
        rowIterator, cardinalityCheck, isTargetRowPresentPred, isSourceRowPresentPred,
        projectTargetCols, matchedActions.asInstanceOf[Seq[Project]], notMatchedActions)
    }

    // null indicates a record must be discarded
    mergeIterator.filter(_ != null)
  }

  trait Action {
    def cond: BasePredicate
  }

  case class Project(cond: BasePredicate, proj: Projection) extends Action {
    def apply(row: InternalRow): InternalRow = proj.apply(row)
  }

  case class Split(cond: BasePredicate, proj: Projection, otherProj: Projection) extends Action {
    def projectRow(row: InternalRow): InternalRow = proj.apply(row)
    def projectExtraRow(row: InternalRow): InternalRow = otherProj.apply(row)
  }

  object EmptyProjection extends Projection {
    override def apply(row: InternalRow): InternalRow = null
  }

  class MergeRowIterator(
      private val rowIterator: Iterator[InternalRow],
      private val cardinalityCheck: CardinalityCheck,
      private val isTargetRowPresentPred: BasePredicate,
      private val isSourceRowPresentPred: BasePredicate,
      private val targetTableProj: Projection,
      private val matchedActions: Seq[Project],
      private val notMatchedActions: Seq[Project])
    extends Iterator[InternalRow] {

    override def hasNext: Boolean = rowIterator.hasNext

    override def next(): InternalRow = {
      val row = rowIterator.next()

      val isSourceRowPresent = isSourceRowPresentPred.eval(row)
      val isTargetRowPresent = isTargetRowPresentPred.eval(row)

      if (isTargetRowPresent && isSourceRowPresent) {
        cardinalityCheck.execute(row)
        applyMatchedActions(row)
      } else if (isSourceRowPresent) {
        applyNotMatchedActions(row)
      } else if (emitNotMatchedTargetRows && isTargetRowPresent) {
        targetTableProj.apply(row)
      } else {
        null
      }
    }

    private def applyMatchedActions(row: InternalRow): InternalRow = {
      for (action <- matchedActions) {
        if (action.cond.eval(row)) {
          return action.apply(row)
        }
      }

      if (emitNotMatchedTargetRows) targetTableProj.apply(row) else null
    }

    private def applyNotMatchedActions(row: InternalRow): InternalRow = {
      for (action <- notMatchedActions) {
        if (action.cond.eval(row)) {
          return action.apply(row)
        }
      }

      null
    }
  }

  /**
   * An iterator that splits updates into deletes and inserts.
   *
   * Each input row that represents an update becomes two output rows: a delete and an insert.
   */
  class SplittingMergeRowIterator(
      private val rowIterator: Iterator[InternalRow],
      private val cardinalityCheck: CardinalityCheck,
      private val isTargetRowPresentPred: BasePredicate,
      private val matchedActions: Seq[Action],
      private val notMatchedActions: Seq[Project])
    extends Iterator[InternalRow] {

    var cachedExtraRow: InternalRow = _

    override def hasNext: Boolean = cachedExtraRow != null || rowIterator.hasNext

    override def next(): InternalRow = {
      if (cachedExtraRow != null) {
        val extraRow = cachedExtraRow
        cachedExtraRow = null
        return extraRow
      }

      val row = rowIterator.next()

      // it should be OK to just check if the target row exists
      // as this iterator is only used for delta-based row-level plans
      // that are rewritten using an inner or right outer join
      if (isTargetRowPresentPred.eval(row)) {
        cardinalityCheck.execute(row)
        applyMatchedActions(row)
      } else {
        applyNotMatchedActions(row)
      }
    }

    private def applyMatchedActions(row: InternalRow): InternalRow = {
      for (action <- matchedActions) {
        if (action.cond.eval(row)) {
          action match {
            case split: Split =>
              cachedExtraRow = split.projectExtraRow(row)
              return split.projectRow(row)
            case project: Project =>
              return project.apply(row)
          }
        }
      }

      null
    }

    private def applyNotMatchedActions(row: InternalRow): InternalRow = {
      for (action <- notMatchedActions) {
        if (action.cond.eval(row)) {
          return action.apply(row)
        }
      }

      null
    }
  }

  sealed trait CardinalityCheck {

    def execute(inputRow: InternalRow): Unit

    protected def fail(): Unit = {
      throw new SparkException(
        "The ON search condition of the MERGE statement matched a single row from " +
        "the target table with multiple rows of the source table. This could result " +
        "in the target row being operated on more than once with an update or delete " +
        "operation and is not allowed.")
    }
  }

  object EmptyCardinalityCheck extends CardinalityCheck {
    def execute(inputRow: InternalRow): Unit = {}
  }

  case class BitmapCardinalityCheck(rowIdOrdinal: Int) extends CardinalityCheck {
    private val matchedRowIds = new Roaring64Bitmap()

    override def execute(inputRow: InternalRow): Unit = {
      val currentRowId = inputRow.getLong(rowIdOrdinal)
      if (matchedRowIds.contains(currentRowId)) {
        fail()
      }
      matchedRowIds.add(currentRowId)
    }
  }
}
