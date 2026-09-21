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
package org.apache.iceberg.spark.vendor;

import org.apache.spark.sql.SparkSessionExtensions;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/** Stands in for a vendor's session extension: the sign_files TVF and the operator that runs it. */
public class FileIcebergSparkSessionExtensions
    extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {

  @Override
  public BoxedUnit apply(SparkSessionExtensions extensions) {
    extensions.injectTableFunction(
        FileBatchFunctions.tableFunction(
            SignFiles.NAME, SignFiles.SCHEMA, SignFiles.class.getName()));
    extensions.injectPlannerStrategy(BatchApplyStrategy::new);
    return BoxedUnit.UNIT;
  }
}
