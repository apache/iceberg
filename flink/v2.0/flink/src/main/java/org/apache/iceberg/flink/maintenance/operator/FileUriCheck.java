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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.actions.FileURI;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class FileUriCheck extends ProcessFunction<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileUriCheck.class);

  private final Map<String, String> equalSchemes;
  private final Map<String, String> equalAuthorities;
  private transient Counter errorCounter;
  private final String taskName;
  private final String tableName;
  private final int taskIndex;

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  public FileUriCheck(
      Map<String, String> equalSchemes,
      Map<String, String> equalAuthorities,
      String tableName,
      String taskName,
      int taskIndex) {
    Preconditions.checkNotNull(equalSchemes, "equalSchemes should no be null");
    Preconditions.checkNotNull(equalAuthorities, "equalAuthorities should no be null");
    this.equalSchemes = equalSchemes;
    this.equalAuthorities = equalAuthorities;
    this.tableName = tableName;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
  }

  @Override
  public void processElement(String value, Context ctx, Collector<String> collector)
      throws Exception {
    try {
      new FileURI(value, equalSchemes, equalAuthorities);
      collector.collect(value);
    } catch (Exception e) {
      LOG.error("Uri convert to FileURI error! Uri is {}.", value, e);
      ctx.output(DeleteOrphanFiles.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }
}
