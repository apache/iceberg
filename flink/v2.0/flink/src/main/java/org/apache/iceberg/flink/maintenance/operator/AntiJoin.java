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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.FileURI;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class AntiJoin extends KeyedCoProcessFunction<String, String, String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(AntiJoin.class);

  private transient ListState<String> foundInTable;
  private transient ValueState<String> foundInFileSystem;
  private final DeleteOrphanFiles.PrefixMismatchMode prefixMismatchMode;
  private final Map<String, String> equalSchemes;
  private final Map<String, String> equalAuthorities;

  public AntiJoin(
      DeleteOrphanFiles.PrefixMismatchMode prefixMismatchMode,
      Map<String, String> equalSchemes,
      Map<String, String> equalAuthorities) {
    this.prefixMismatchMode = prefixMismatchMode;
    this.equalSchemes = equalSchemes;
    this.equalAuthorities = equalAuthorities;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    foundInTable =
        getRuntimeContext()
            .getListState(new ListStateDescriptor<>("antiJoinFoundInTable", Types.STRING));
    foundInFileSystem =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("antiJoinFoundInFileSystem", Types.STRING));
  }

  @Override
  public void processElement1(String value, Context context, Collector<String> collector)
      throws Exception {
    foundInTable.add(value);
    context.timerService().registerEventTimeTimer(context.timestamp());
  }

  @Override
  public void processElement2(String value, Context context, Collector<String> collector)
      throws Exception {
    foundInFileSystem.update(value);
    context.timerService().registerEventTimeTimer(context.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    List<FileURI> foundInTablesList = Lists.newArrayList();
    for (String uri : foundInTable.get()) {
      foundInTablesList.add(new FileURI(uri, equalSchemes, equalAuthorities));
    }

    if (foundInFileSystem.value() != null && foundInTablesList.isEmpty()) {
      FileURI fileURI = new FileURI(foundInFileSystem.value(), equalSchemes, equalAuthorities);
      out.collect(fileURI.getUriAsString());
    } else if (foundInFileSystem.value() != null && !foundInTablesList.isEmpty()) {
      FileURI actual = new FileURI(foundInFileSystem.value(), equalSchemes, equalAuthorities);
      boolean found = true;
      for (FileURI valid : foundInTablesList) {
        if (valid.schemeMatch(actual) && valid.authorityMatch(actual)) {
          found = false;
          break;
        }
      }

      if (found) {
        if (prefixMismatchMode == DeleteOrphanFiles.PrefixMismatchMode.DELETE) {
          out.collect(foundInFileSystem.value());
        } else if (prefixMismatchMode == DeleteOrphanFiles.PrefixMismatchMode.ERROR) {
          ValidationException validationException =
              new ValidationException(
                  "Unable to determine whether certain files are orphan. "
                      + "Metadata references files that match listed/provided files except for authority/scheme. "
                      + "Please, inspect the conflicting authorities/schemes and provide which of them are equal "
                      + "by further configuring the action via equalSchemes() and equalAuthorities() methods. "
                      + "Set the prefix mismatch mode to 'NONE' to ignore remaining locations with conflicting "
                      + "authorities/schemes or to 'DELETE' iff you are ABSOLUTELY confident that remaining conflicting "
                      + "authorities/schemes are different. It will be impossible to recover deleted files. "
                      + "Conflicting authorities/schemes");
          LOG.error(
              "Unable to determine whether certain files are orphan. Found in filesystem: {} and in table: {}",
              actual,
              StringUtils.join(foundInTablesList, ","),
              validationException);
          ctx.output(
              org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.ERROR_STREAM,
              validationException);
        }
      }
    }

    foundInTable.clear();
    foundInFileSystem.clear();
  }
}
