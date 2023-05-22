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
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

case class UpdateRowsExec(
    deleteOutput: Seq[Expression],
    insertOutput: Seq[Expression],
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  assert(deleteOutput.size == output.size)
  assert(insertOutput.size == output.size)

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  override def simpleString(maxFields: Int): String = {
    s"UpdateRowsExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions(processPartition)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override def usedInputs: AttributeSet = {
    // only attributes used at least twice should be evaluated before this plan,
    // otherwise defer the evaluation until an attribute is actually used
    val usedExprIds = insertOutput.flatMap(_.collect {
      case attr: Attribute => attr.exprId
    })
    val usedMoreThanOnceExprIds = usedExprIds.groupBy(id => id).filter(_._2.size > 1).keySet
    references.filter(attr => usedMoreThanOnceExprIds.contains(attr.exprId))
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // no need to perform sub expression elimination for the delete projection as
    // as it only outputs row ID and metadata attributes without any changes
    val deleteExprs = BindReferences.bindReferences(deleteOutput, child.output)
    val deleteOutputVars = deleteExprs.map(_.genCode(ctx))

    val insertExprs = BindReferences.bindReferences(insertOutput, child.output)
    val (insertSubExprsCode, insertOutputVars, insertLocalInputVars) =
      if (conf.subexpressionEliminationEnabled) {
        val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(insertExprs)
        val subExprsCode = ctx.evaluateSubExprEliminationState(subExprs.states.values)
        val vars = ctx.withSubExprEliminationExprs(subExprs.states) {
          insertExprs.map(_.genCode(ctx))
        }
        val localInputVars = subExprs.exprCodesNeedEvaluate
        (subExprsCode, vars, localInputVars)
      } else {
        ("", insertExprs.map(_.genCode(ctx)), Seq.empty)
      }

    val nonDeterministicInsertAttrs = insertOutput.zip(output)
      .collect { case (expr, attr) if !expr.deterministic => attr }
    val nonDeterministicInsertAttrSet = AttributeSet(nonDeterministicInsertAttrs)

    s"""
       |// generate DELETE record
       |${consume(ctx, deleteOutputVars)}
       |// generate INSERT records
       |${evaluateVariables(insertLocalInputVars)}
       |$insertSubExprsCode
       |${evaluateRequiredVariables(output, insertOutputVars, nonDeterministicInsertAttrSet)}
       |${consume(ctx, insertOutputVars)}
     """.stripMargin
  }

  private def processPartition(rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val deleteProj = createProjection(deleteOutput)
    val insertProj = createProjection(insertOutput)
    new UpdateAsDeleteAndInsertRowIterator(rowIterator, deleteProj, insertProj)
  }

  private def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, child.output)
  }

  class UpdateAsDeleteAndInsertRowIterator(
      private val inputRows: Iterator[InternalRow],
      private val deleteProj: UnsafeProjection,
      private val insertProj: UnsafeProjection)
    extends Iterator[InternalRow] {

    var cachedInsertRow: InternalRow = _

    override def hasNext: Boolean = cachedInsertRow != null || inputRows.hasNext

    override def next(): InternalRow = {
      if (cachedInsertRow != null) {
        val insertRow = cachedInsertRow
        cachedInsertRow = null
        return insertRow
      }

      val row = inputRows.next()
      cachedInsertRow = insertProj.apply(row)
      deleteProj.apply(row)
    }
  }
}
