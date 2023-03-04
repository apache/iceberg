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
package org.apache.iceberg.flink.sink.shuffle;

import java.io.Serializable;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatistics;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The wrapper class for data statistics and record. It is the only way for data statistics operator to send
 * global data statistics to custom partitioner to distribute data based on statistics
 *
 * <p>DataStatisticsOrRecord is sent from {@link DataStatisticsOperator} to partitioner. It
 * contains either data statistics(globally aggregated) or a record. Once partitioner receives the data
 * statistics, it will use that to decide the coming record should send to which writer subtask. After
 * shuffling, a filter and mapper are required to filter out the data distribution weight, unwrap the
 * object and extract the original record type T.
 */
public class DataStatisticsOrRecord<T, K> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final DataStatistics<K> statistics;
  private final T record;

  private DataStatisticsOrRecord(T record, DataStatistics<K> statistics) {
    Preconditions.checkArgument(
        record != null ^ statistics != null,
        "A DataStatisticsOrRecord contain either statistics or record, not neither or both");
    this.statistics = statistics;
    this.record = record;
  }

  static <T, K> DataStatisticsOrRecord<T, K> fromRecord(T record) {
    return new DataStatisticsOrRecord<>(record, null);
  }

  static <T, K> DataStatisticsOrRecord<T, K> fromDataStatistics(DataStatistics<K> statistics) {
    return new DataStatisticsOrRecord<>(null, statistics);
  }

  boolean hasDataStatistics() {
    return statistics != null;
  }

  boolean hasRecord() {
    return record != null;
  }

  DataStatistics<K> dataStatistics() {
    return statistics;
  }

  T record() {
    return record;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("statistics", statistics)
        .add("record", record)
        .toString();
  }
}
