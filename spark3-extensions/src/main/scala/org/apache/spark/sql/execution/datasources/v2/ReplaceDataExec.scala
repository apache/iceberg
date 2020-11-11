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
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.execution.SparkPlan

case class ReplaceDataExec(batchWrite: BatchWrite, queryExec: SparkPlan) extends V2TableWriteExec {

  // override child so that we can properly plan queryExec without executing the file filter
  override def child: SparkPlan = queryExec

  // TODO: doPrepare is NOT invoked by V2TableWriteExec
  override lazy val query: SparkPlan = {
    queryExec.prepare()
    queryExec
  }

  override protected def run(): Seq[InternalRow] = {
    writeWithV2(batchWrite)
  }
}
