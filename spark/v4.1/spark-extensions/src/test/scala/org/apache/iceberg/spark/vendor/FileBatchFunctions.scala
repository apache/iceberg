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
package org.apache.iceberg.spark.vendor

import org.apache.iceberg.rest.RESTCatalogProperties
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.util.PropertyUtil
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.FunctionTableSubqueryArgumentExpression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

/**
 * Builds table-valued functions of the form {@code name(TABLE(t), 'file_column'[, batch_size])}
 * that apply a {@link FileBatchProcessor} to the named file column in batches, using the FileIO
 * of the one Iceberg table inside {@code TABLE()}. The batch size defaults to that FileIO's
 * signing batch maximum and may not exceed it.
 */
object FileBatchFunctions {

  type TableFunction = (FunctionIdentifier, ExpressionInfo, Seq[Expression] => LogicalPlan)

  def tableFunction(
      name: String,
      outputSchema: StructType,
      processorClass: String): TableFunction = {
    (
      FunctionIdentifier(name),
      new ExpressionInfo(processorClass, name),
      (args: Seq[Expression]) => build(name, outputSchema, processorClass, args))
  }

  def instantiate(className: String): FileBatchProcessor = {
    Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
      .loadClass(className)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[FileBatchProcessor]
  }

  private def build(
      name: String,
      outputSchema: StructType,
      processorClass: String,
      args: Seq[Expression]): LogicalPlan = {
    require(
      args.size == 2 || args.size == 3,
      s"$name takes a table, a file column name and an optional batch size, " +
        s"got ${args.size} arguments")

    val child = args.head match {
      case table: FunctionTableSubqueryArgumentExpression => table.plan
      case other => throw new IllegalArgumentException(s"Not a TABLE() argument: $other")
    }

    val column = args(1) match {
      case Literal(value, StringType) => UnresolvedAttribute.parseAttributeName(value.toString)
      case other => throw new IllegalArgumentException(s"Not a column name: $other")
    }

    val io = child.collect { case relation: DataSourceV2Relation => relation.table } match {
      case Seq(table: SparkTable) => table.table().io()
      case Seq() => throw new IllegalArgumentException(s"$name: TABLE() holds no Iceberg table")
      case tables =>
        throw new IllegalArgumentException(
          s"$name: TABLE() holds ${tables.size} tables, expected one")
    }

    val maxBatchSize = PropertyUtil.propertyAsInt(
      io.properties(),
      RESTCatalogProperties.REMOTE_SIGNING_BATCH_MAX_SIZE,
      RESTCatalogProperties.REMOTE_SIGNING_BATCH_MAX_SIZE_DEFAULT)

    val batchSize = args.lift(2) match {
      case None => maxBatchSize
      case Some(Literal(size, IntegerType)) =>
        val requested = size.asInstanceOf[Int]
        require(
          requested > 0 && requested <= maxBatchSize,
          s"$name: batch size $requested is not in 1..$maxBatchSize, the signing batch maximum")
        requested
      case Some(other) => throw new IllegalArgumentException(s"Not a batch size: $other")
    }

    BatchApply(
      Project(Seq(Alias(UnresolvedAttribute(column), column.last)()), child),
      name,
      processorClass,
      batchSize,
      DataTypeUtils.toAttributes(outputSchema),
      io)
  }
}
