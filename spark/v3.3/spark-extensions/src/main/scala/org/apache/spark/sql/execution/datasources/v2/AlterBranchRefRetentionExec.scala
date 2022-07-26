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

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.AlterBranchRefRetention
import org.apache.spark.sql.connector.catalog._

case class AlterBranchRefRetentionExec(
                                        catalog: TableCatalog,
                                        ident: Identifier,
                                        alterBranchRef: AlterBranchRefRetention) extends LeafV2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
          if(alterBranchRef.snapshotRefRetain.nonEmpty) {
            val ms =  iceberg.table.manageSnapshots()
            ms.setMaxRefAgeMs(alterBranchRef.branch, alterBranchRef.snapshotRefRetain.get)
            ms.commit()
          }
      case table =>
        throw new UnsupportedOperationException(s"alter branch retention to non-Iceberg table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"Alter branch:${alterBranchRef.branch} reference life cycle for table:${ident.quoted}"
  }
}
