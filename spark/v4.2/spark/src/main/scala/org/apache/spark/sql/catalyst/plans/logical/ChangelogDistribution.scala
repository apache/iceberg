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

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.{Dataset => ClassicDataset}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}

case class ChangelogDistribution(keys: Seq[Expression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

object ChangelogDistribution {
  def prepare(df: Dataset[Row], columns: Array[Column]): Dataset[Row] = {
    if (Spark3Util.extensionsEnabled(df.sparkSession)) {
      val keys = df.select(columns: _*).queryExecution.analyzed.output
      ClassicDataset.ofRows(
        df.sparkSession.asInstanceOf[ClassicSparkSession],
        ChangelogDistribution(keys, df.queryExecution.optimizedPlan))
    } else {
      df.repartition(columns: _*)
    }
  }
}
