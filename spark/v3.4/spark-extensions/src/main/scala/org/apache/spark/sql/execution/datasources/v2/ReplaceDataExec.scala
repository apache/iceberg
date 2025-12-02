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

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan

/**
 * Physical plan node to replace data in existing tables.
 */
case class ReplaceDataExec(query: SparkPlan, refreshCache: () => Unit, write: Write)
    extends V2ExistingTableWriteExec {

  override lazy val references: AttributeSet = query.outputSet
  override lazy val stringArgs: Iterator[Any] = Iterator(query, write)

  override protected def withNewChildInternal(newChild: SparkPlan): ReplaceDataExec = {
    copy(query = newChild)
  }
}
