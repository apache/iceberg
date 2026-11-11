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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkCdcScanBuilder extends BaseSparkScanBuilder {

  private final SparkChangelogRange range;

  SparkCdcScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      CaseInsensitiveStringMap options,
      SparkChangelogRange range) {
    super(spark, table, schema, options);
    this.range = range;
  }

  @Override
  public Scan build() {
    Schema readProjection = projection();
    IncrementalChangelogScan scan =
        table()
            .newIncrementalChangelogScan()
            .caseSensitive(caseSensitive())
            .project(ChangelogUtil.changelogSchema(SparkChangelog.dropCdcMetadata(readProjection)))
            .metricsReporter(metricsReporter());
    scan = configureSplitPlanning(scan);
    return new SparkCdcScan(spark(), table(), scan, readConf(), readProjection, range);
  }
}
