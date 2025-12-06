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

import org.apache.iceberg.NullOrder
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.ReplaceSortOrder
import org.apache.iceberg.Schema
import org.apache.iceberg.SortDirection
import org.apache.iceberg.SortOrder
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.transforms.SortOrderVisitor
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructField
import scala.jdk.CollectionConverters._

case class CreateV2TableLikeExec(
    catalog: TableCatalog,
    ident: Identifier,
    sourceCatalog: TableCatalog,
    sourceIdent: Identifier,
    tableProps: Map[String, String],
    ignoreIfExists: Boolean)
    extends LeafV2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    if (ignoreIfExists && catalog.tableExists(ident)) {
      return Nil
    }

    val sourceTable = sourceCatalog.loadTable(sourceIdent) match {
      case iceberg: SparkTable => iceberg
      case table =>
        throw new UnsupportedOperationException(s"Cannot create table like for non-Iceberg table: $table")
    }

    val schema: Schema = sourceTable.table().schema()
    val columns = SparkSchemaUtil.convert(schema).fields.map {
      case StructField(name, dataType, nullable, _) =>
        Column.create(name, dataType, nullable)
    }

    val partitionSpec: PartitionSpec = sourceTable.table().spec()
    val partitioning: Array[Transform] = Spark3Util.toTransforms(partitionSpec)
    val sourceProps = sourceTable.table().properties().asScala.toMap
    val mergedProps = sourceProps ++ tableProps

    catalog.createTable(ident, columns, partitioning, mergedProps.asJava)

    // Copy sort order from source table if it exists
    val sourceSortOrder: SortOrder = sourceTable.table().sortOrder()
    if (sourceSortOrder.isSorted) {
      catalog.loadTable(ident) match {
        case createdTable: SparkTable =>
          val txn = createdTable.table().newTransaction()
          val orderBuilder = txn.replaceSortOrder()
          SortOrderVisitor.visit(sourceSortOrder, new ApplySortOrder(orderBuilder))
          orderBuilder.commit()
          txn.commitTransaction()
        case _ => // ignore if not a SparkTable
      }
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateTableLike ${catalog.name}.${ident.quoted} LIKE ${sourceCatalog.name}.${sourceIdent.quoted}"
  }

  /**
   * Visitor that applies sort fields from a source SortOrder to a ReplaceSortOrder builder.
   */
  private class ApplySortOrder(orderBuilder: ReplaceSortOrder) extends SortOrderVisitor[Void] {

    override def field(
        sourceName: String,
        sourceId: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.ref(sourceName), direction, nullOrder)
    }

    override def bucket(
        sourceName: String,
        sourceId: Int,
        width: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.bucket(sourceName, width), direction, nullOrder)
    }

    override def truncate(
        sourceName: String,
        sourceId: Int,
        width: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.truncate(sourceName, width), direction, nullOrder)
    }

    override def year(
        sourceName: String,
        sourceId: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.year(sourceName), direction, nullOrder)
    }

    override def month(
        sourceName: String,
        sourceId: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.month(sourceName), direction, nullOrder)
    }

    override def day(
        sourceName: String,
        sourceId: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.day(sourceName), direction, nullOrder)
    }

    override def hour(
        sourceName: String,
        sourceId: Int,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      addTerm(Expressions.hour(sourceName), direction, nullOrder)
    }

    private def addTerm(
        term: org.apache.iceberg.expressions.Term,
        direction: SortDirection,
        nullOrder: NullOrder): Void = {
      direction match {
        case SortDirection.ASC => orderBuilder.asc(term, nullOrder)
        case SortDirection.DESC => orderBuilder.desc(term, nullOrder)
      }

      null
    }
  }
}
