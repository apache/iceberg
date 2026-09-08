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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Skip file deletion processing when an error is encountered. */
@Internal
public class SkipOnError extends AbstractStreamOperator<String>
    implements TwoInputStreamOperator<String, Exception, String> {
  private static final Logger LOG = LoggerFactory.getLogger(SkipOnError.class);
  private transient ListState<String> filesToDelete;
  private transient ListState<Boolean> hasError;
  private boolean hasErrorFlag = false;

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.filesToDelete =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("blockOnErrorFiles", String.class));
    this.hasError =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("blockOnErrorHasError", Types.BOOLEAN));

    if (!Iterables.isEmpty(hasError.get())) {
      hasErrorFlag = true;
    }
  }

  @Override
  public void processElement1(StreamRecord<String> element) throws Exception {
    if (!hasErrorFlag) {
      filesToDelete.add(element.getValue());
    }
  }

  @Override
  public void processElement2(StreamRecord<Exception> element) throws Exception {
    hasError.add(true);
    hasErrorFlag = true;
    filesToDelete.clear();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try {
      if (!hasErrorFlag) {
        filesToDelete.get().forEach(file -> output.collect(new StreamRecord<>(file)));
      } else {
        LOG.info("Omitting result on failure at {}", mark.getTimestamp());
      }
    } finally {
      filesToDelete.clear();
      hasError.clear();
      hasErrorFlag = false;
    }

    super.processWatermark(mark);
  }
}
