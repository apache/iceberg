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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.iceberg.NullOrder
import org.apache.iceberg.Schema
import org.apache.iceberg.SortDirection
import org.apache.iceberg.SortOrder
import org.apache.iceberg.expressions.Term

class SortOrderParserUtil {

  def collectSortOrder(
      tableSchema: Schema,
      sortOrder: Seq[(Term, SortDirection, NullOrder)]): SortOrder = {
    val orderBuilder = SortOrder.builderFor(tableSchema)
    sortOrder.foreach {
      case (term, SortDirection.ASC, nullOrder) =>
        orderBuilder.asc(term, nullOrder)
      case (term, SortDirection.DESC, nullOrder) =>
        orderBuilder.desc(term, nullOrder)
    }
    orderBuilder.build();
  }
}
