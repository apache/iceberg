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
package org.apache.iceberg.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.util.ThreadPools;

/**
 * When constructing Flink Iceberg source via Java API, configs can be set in {@link Configuration}
 * passed to source builder. E.g.
 *
 * <pre>
 *   configuration.setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, true);
 *   FlinkSource.forRowData()
 *       .flinkConf(configuration)
 *       ...
 * </pre>
 *
 * <p>When using Flink SQL/table API, connector options can be set in Flink's {@link
 * TableEnvironment}.
 *
 * <pre>
 *   TableEnvironment tEnv = createTableEnv();
 *   tEnv.getConfig()
 *        .getConfiguration()
 *        .setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, true);
 * </pre>
 */
public class FlinkConfigOptions {

  private FlinkConfigOptions() {}

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM =
      ConfigOptions.key("table.exec.iceberg.infer-source-parallelism")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "If is false, parallelism of source are set by config.\n"
                  + "If is true, source parallelism is inferred according to splits number.\n");

  public static final ConfigOption<Integer> TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX =
      ConfigOptions.key("table.exec.iceberg.infer-source-parallelism.max")
          .intType()
          .defaultValue(100)
          .withDescription("Sets max infer parallelism for source operator.");

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_EXPOSE_SPLIT_LOCALITY_INFO =
      ConfigOptions.key("table.exec.iceberg.expose-split-locality-info")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Expose split host information to use Flink's locality aware split assigner.");

  public static final ConfigOption<Integer> SOURCE_READER_FETCH_BATCH_RECORD_COUNT =
      ConfigOptions.key("table.exec.iceberg.fetch-batch-record-count")
          .intType()
          .defaultValue(2048)
          .withDescription("The target number of records for Iceberg reader fetch batch.");

  public static final ConfigOption<Integer> TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE =
      ConfigOptions.key("table.exec.iceberg.worker-pool-size")
          .intType()
          .defaultValue(ThreadPools.WORKER_THREAD_POOL_SIZE)
          .withDescription("The size of workers pool used to plan or scan manifests.");

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE =
      ConfigOptions.key("table.exec.iceberg.use-flip27-source")
          .booleanType()
          .defaultValue(false)
          .withDescription("Use the FLIP-27 based Iceberg source implementation.");

  public static final ConfigOption<SplitAssignerType> TABLE_EXEC_SPLIT_ASSIGNER_TYPE =
      ConfigOptions.key("table.exec.iceberg.split-assigner-type")
          .enumType(SplitAssignerType.class)
          .defaultValue(SplitAssignerType.SIMPLE)
          .withDescription(
              Description.builder()
                  .text("Split assigner type that determine how splits are assigned to readers.")
                  .linebreak()
                  .list(
                      TextElement.text(
                          SplitAssignerType.SIMPLE
                              + ": simple assigner that doesn't provide any guarantee on order or locality."))
                  .build());
}
