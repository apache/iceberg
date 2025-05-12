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
package org.apache.iceberg.connect.data.routers;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.connect.RecordRouter;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.connect.sink.SinkRecord;

public class AllTableRecordRouter implements RecordRouter {

  private final List<String> allTables = Lists.newArrayList();

  @Override
  public List<String> tables(SinkRecord record) {
    return allTables;
  }

  @Override
  public void configure(Map<String, String> props) {
    if (props.getOrDefault(RouterConstants.TABLES_PROP, "").isBlank()) {
      return;
    }
    allTables.addAll(Splitter.on(",").splitToList(props.get(RouterConstants.TABLES_PROP)));
  }
}
