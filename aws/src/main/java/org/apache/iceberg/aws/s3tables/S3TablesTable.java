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
package org.apache.iceberg.aws.s3tables;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.metrics.MetricsReporter;

/**
 * A simple wrapper around BaseTable to demarcate S3 Tables tables. Some engines use this to detect
 * the type of the table and apply S3 Tables-specific behavior.
 */
public class S3TablesTable extends BaseTable {
  public S3TablesTable(TableOperations ops, String name, MetricsReporter reporter) {
    super(ops, name, reporter);
  }
}
