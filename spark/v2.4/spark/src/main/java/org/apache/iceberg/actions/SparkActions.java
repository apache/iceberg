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
package org.apache.iceberg.actions;

import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;

/**
 * @deprecated since 0.12.0, used for supporting {@link RewriteDataFilesAction} in Spark 2.4 for
 *     backward compatibility. This implementation is no longer maintained, the new implementation
 *     is available with Spark 3.x
 */
@Deprecated
class SparkActions extends Actions {
  protected SparkActions(SparkSession spark, Table table) {
    super(spark, table);
  }
}
