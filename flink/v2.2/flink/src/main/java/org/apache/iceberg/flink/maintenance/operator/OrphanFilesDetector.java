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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.FileURI;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized co-process function that performs an anti-join between two streams of file URIs.
 *
 * <p>Emits every file that exists in the file system but is not referenced in the table metadata,
 * which are considered orphan files. It also handles URI normalization using provided scheme and
 * authority equivalence mappings.
 */
@Internal
public class OrphanFilesDetector extends KeyedCoProcessFunction<String, String, String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(OrphanFilesDetector.class);

  // Use MapState to dedupe the strings found in the table
  private transient MapState<String, Boolean> foundInTable;
  private transient ValueState<String> foundInFileSystem;
  private transient ValueState<Boolean> hasUriError;
  private final DeleteOrphanFiles.PrefixMismatchMode prefixMismatchMode;
  private final Map<String, String> equalSchemes;
  private final Map<String, String> equalAuthorities;

  public OrphanFilesDetector(
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
            .getMapState(
                new MapStateDescriptor<>("antiJoinFoundInTable", Types.STRING, Types.BOOLEAN));
    hasUriError =
        getRuntimeContext().getState(new ValueStateDescriptor<>("antiJoinUriError", Types.BOOLEAN));
    foundInFileSystem =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("antiJoinFoundInFileSystem", Types.STRING));
  }

  @Override
  public void processElement1(String value, Context context, Collector<String> collector)
      throws Exception {
    if (shouldSkipElement(value, context)) {
      return;
    }

    if (!foundInTable.contains(value)) {
      foundInTable.put(value, true);
      context.timerService().registerEventTimeTimer(context.timestamp());
    }
  }

  @Override
  public void processElement2(String value, Context context, Collector<String> collector)
      throws Exception {
    if (shouldSkipElement(value, context)) {
      return;
    }

    foundInFileSystem.update(value);
    context.timerService().registerEventTimeTimer(context.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    if (Boolean.TRUE.equals(hasUriError.value())) {
      clearState();
      return;
    }

    List<FileURI> foundInTablesList = Lists.newArrayList();
    foundInTable
        .keys()
        .forEach(
            uri ->
                foundInTablesList.add(
                    new FileURI(new Path(uri).toUri(), equalSchemes, equalAuthorities)));

    if (foundInFileSystem.value() != null) {
      if (foundInTablesList.isEmpty()) {
        FileURI fileURI =
            new FileURI(
                new Path(foundInFileSystem.value()).toUri(), equalSchemes, equalAuthorities);
        out.collect(fileURI.getUriAsString());
      } else {
        FileURI actual =
            new FileURI(
                new Path(foundInFileSystem.value()).toUri(), equalSchemes, equalAuthorities);
        if (hasMismatch(actual, foundInTablesList)) {
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
                        + "authorities/schemes or to 'DELETE' if you are ABSOLUTELY confident that remaining conflicting "
                        + "authorities/schemes are different. It will be impossible to recover deleted files. "
                        + "Conflicting authorities/schemes");
            LOG.warn(
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
    }

    clearState();
  }

  private boolean hasMismatch(FileURI actual, List<FileURI> foundInTablesList) {
    return foundInTablesList.stream()
        .noneMatch(valid -> valid.schemeMatch(actual) && valid.authorityMatch(actual));
  }

  private boolean shouldSkipElement(String value, Context context) throws IOException {
    if (Boolean.TRUE.equals(hasUriError.value())) {
      return true;
    }

    if (FileUriKeySelector.INVALID_URI.equals(context.getCurrentKey())) {
      context.output(
          org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.ERROR_STREAM,
          new RuntimeException("Invalid URI format detected: " + value));
      hasUriError.update(true);
      foundInTable.clear();
      foundInFileSystem.clear();
      return true;
    }

    return false;
  }

  private void clearState() {
    hasUriError.clear();
    foundInTable.clear();
    foundInFileSystem.clear();
  }
}
