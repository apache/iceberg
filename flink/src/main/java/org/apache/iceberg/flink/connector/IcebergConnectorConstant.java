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

package org.apache.iceberg.flink.connector;

import java.util.concurrent.TimeUnit;

public class IcebergConnectorConstant {
  private IcebergConnectorConstant() {}

  public static final String TYPE = "iceberg";

  public static final String ICEBERG_APP_TYPE = "flink";

  public static final String CATALOG_TYPE = "catalog_type";
  public static final String HIVE_CATALOG = "HIVE";
  public static final String HADOOP_CATALOG = "HADOOP";
  public static final String CATALOG_TYPE_DEFAULT = HIVE_CATALOG;
  public static final String HADOOP_CATALOG_WAREHOUSE_LOCATION = "hadoop_catalog_warehouse_location";

  public static final String NAMESPACE = "namespace";
  public static final String TABLE = "table";

  public static final String FORMAT = "format";
  public static final String CODEC = "codec";

  public static final String SKIP_INCOMPATIBLE_RECORD = "skip_incompatible_record";
  public static final String VTTS_WATERMARK_TIMESTAMP_FIELD = "vtts_watermark_timestamp_field";
  public static final String VTTS_WATERMARK_TIMESTAMP_UNIT = "vtts_watermark_timestamp_unit";
  public static final String SNAPSHOT_RETENTION_HOURS = "snapshot_retention_hours";
  public static final String COMMIT_RESTORED_MANIFEST_FILES = "commit_restored_manifest_files";
  public static final String MAX_FILE_SIZE = "max_file_size";

  public static final boolean DEFAULT_SKIP_INCOMPATIBLE_RECORD = false;
  public static final String DEFAULT_VTTS_WATERMARK_TIMESTAMP_UNIT = TimeUnit.MILLISECONDS.name();
  public static final long DEFAULT_SNAPSHOT_RETENTION_HOURS = 70;
  public static final boolean DEFAULT_COMMIT_RESTORED_MANIFEST_FILES = true;
  public static final long DEFAULT_MAX_FILE_SIZE = 1024L * 1024L * 1024L * 4;

  public static final String SINK_TAG_KEY = "sink";
  public static final String OUTPUT_TAG_KEY = "output";
  public static final String OUTPUT_CLUSTER_TAG_KEY = "outputCluster";

  public static final String SUBTASK_ID = "subtask_id";
  public static final String EXCEPTION_CLASS = "exception_class";
}
