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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog

case class ResolveViews(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {
  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UnresolvedTableOrView(CatalogAndIdentifier(catalog, ident), _, _, _) =>
      ViewUtil
        .loadView(catalog, ident)
        .map(view => ResolvedV2View(catalog.asViewCatalog, ident, view))
        .getOrElse(u)
  }
}
