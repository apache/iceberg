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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.BranchOptions
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog

case class CreateOrReplaceBranchExec(
    catalog: TableCatalog,
    ident: Identifier,
    branch: String,
    branchOptions: BranchOptions,
    create: Boolean,
    replace: Boolean,
    ifNotExists: Boolean) extends V2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
        val snapshotId: java.lang.Long = branchOptions.snapshotId
          .orElse(Option(iceberg.table.currentSnapshot()).map(_.snapshotId()))
          .map(java.lang.Long.valueOf)
          .orNull

        val manageSnapshots = iceberg.table().manageSnapshots()
        val refExists = null != iceberg.table().refs().get(branch)

        def safeCreateBranch(): Unit = {
          if (snapshotId == null) {
            manageSnapshots.createBranch(branch)
          } else {
            manageSnapshots.createBranch(branch, snapshotId)
          }
        }

        if (create && replace && !refExists) {
          safeCreateBranch()
        } else if (replace) {
          Preconditions.checkArgument(snapshotId != null,
            "Cannot complete replace branch operation on %s, main has no snapshot", ident)
          manageSnapshots.replaceBranch(branch, snapshotId)
        } else {
          if (refExists && ifNotExists) {
            return Nil
          }

          safeCreateBranch()
        }

        if (branchOptions.numSnapshots.nonEmpty) {
          manageSnapshots.setMinSnapshotsToKeep(branch, branchOptions.numSnapshots.get.toInt)
        }

        if (branchOptions.snapshotRetain.nonEmpty) {
          manageSnapshots.setMaxSnapshotAgeMs(branch, branchOptions.snapshotRetain.get)
        }

        if (branchOptions.snapshotRefRetain.nonEmpty) {
          manageSnapshots.setMaxRefAgeMs(branch, branchOptions.snapshotRefRetain.get)
        }

        manageSnapshots.commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot create or replace branch on non-Iceberg table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateOrReplace branch: $branch for table: ${ident.quoted}"
  }
}
