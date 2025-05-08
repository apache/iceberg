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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AntiJoin extends KeyedCoProcessFunction<String, FileURI, FileURI, String> {
  private static final Logger LOG = LoggerFactory.getLogger(AntiJoin.class);

  private transient ValueState<FileURI> foundInTable;
  private transient ValueState<FileURI> foundInFileSystem;
  private final DeleteOrphanFiles.PrefixMismatchMode prefixMismatchMode;

  public AntiJoin(DeleteOrphanFiles.PrefixMismatchMode prefixMismatchMode) {
    this.prefixMismatchMode = prefixMismatchMode;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    foundInTable =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(
                    "antiJoinFoundInTable", TypeInformation.of(FileURI.class)));
    foundInFileSystem =
        getRuntimeContext()
            .getState(
                new ValueStateDescriptor<>(
                    "antiJoinFoundInFileSystem", TypeInformation.of(FileURI.class)));
  }

  @Override
  public void processElement1(FileURI value, Context context, Collector<String> collector)
      throws Exception {
    foundInTable.update(value);
    context.timerService().registerEventTimeTimer(context.timestamp());
  }

  @Override
  public void processElement2(FileURI value, Context context, Collector<String> collector)
      throws Exception {
    foundInFileSystem.update(value);
    context.timerService().registerEventTimeTimer(context.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    if (foundInFileSystem.value() != null && foundInTable.value() == null) {
      out.collect(foundInFileSystem.value().uriAsString());
    } else if (foundInFileSystem.value() != null && foundInTable.value() != null) {
      FileURI valid = foundInTable.value();
      FileURI actual = foundInFileSystem.value();
      boolean schemeMatch = uriComponentMatch(valid.scheme(), actual.scheme());
      boolean authorityMatch = uriComponentMatch(valid.authority(), actual.authority());

      if ((!schemeMatch || !authorityMatch)
          && prefixMismatchMode == DeleteOrphanFiles.PrefixMismatchMode.DELETE) {
        out.collect(foundInFileSystem.value().uriAsString());
      } else if (prefixMismatchMode == DeleteOrphanFiles.PrefixMismatchMode.ERROR) {
        if (!schemeMatch || !authorityMatch) {
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
              valid,
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

  private boolean uriComponentMatch(String valid, String actual) {
    return Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual);
  }
}
