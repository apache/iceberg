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

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.connector.expressions.Transform

case class ReplacePartitionFieldExec(
    catalog: TableCatalog,
    ident: Identifier,
    transformFrom: Transform,
    transformTo: Transform,
    name: Option[String])
    extends LeafV2CommandExec {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
        val schema = iceberg.table.schema
        transformFrom match {
          case IdentityTransform(FieldReference(parts))
              if parts.size == 1 && schema.findField(parts.head) == null =>
            // the name is not present in the Iceberg schema, so it must be a partition field name, not a column name
            iceberg.table
              .updateSpec()
              .removeField(parts.head)
              .addField(name.orNull, Spark3Util.toIcebergTerm(transformTo))
              .commit()

          case _ =>
            iceberg.table
              .updateSpec()
              .removeField(Spark3Util.toIcebergTerm(transformFrom))
              .addField(name.orNull, Spark3Util.toIcebergTerm(transformTo))
              .commit()
        }

      case table =>
        throw new UnsupportedOperationException(
          s"Cannot replace partition field in non-Iceberg table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"ReplacePartitionField ${catalog.name}.${ident.quoted} ${transformFrom.describe} " +
      s"with ${name.map(n => s"$n=").getOrElse("")}${transformTo.describe}"
  }
}
