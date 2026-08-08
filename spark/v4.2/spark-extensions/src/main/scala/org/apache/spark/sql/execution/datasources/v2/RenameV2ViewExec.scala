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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.execution.command.CommandUtils
import scala.util.Try

case class RenameV2ViewExec(catalog: ViewCatalog, oldIdent: Identifier, newIdent: Identifier)
    extends LeafV2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val oldQualified = (catalog.name() +: oldIdent.asMultipartIdentifier).quoted
    val storageLevel = Try(session.table(oldQualified)).toOption.flatMap { relation =>
      session.sharedState.cacheManager
        .lookupCachedData(relation)
        .map(_.cachedRepresentation.cacheBuilder.storageLevel)
    }

    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, oldIdent))
    catalog.invalidateView(oldIdent)
    catalog.renameView(oldIdent, newIdent)

    storageLevel.foreach { level =>
      val newQualified = (catalog.name() +: newIdent.asMultipartIdentifier).quoted
      session.catalog.cacheTable(newQualified, level)
    }

    Seq.empty
  }

  override def simpleString(maxFields: Int): String = {
    s"RenameV2View $oldIdent to $newIdent"
  }
}
