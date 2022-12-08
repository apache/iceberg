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
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The wrapper class for record and data distribution weight
 *
 * <p>ShuffleRecordWrapper is sent from ShuffleOperator to partitioner. It may contain a record or
 * data distribution weight. Once partitioner receives the weight, it will use that to decide the
 * coming record should send to which writer subtask. After shuffling, a filter and mapper are
 * required to filter out the data distribution weight, unwrap the object and extract the original
 * record type T.
 */
@Internal
public class ShuffleRecordWrapper<T, K extends Serializable> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<K, Long> globalDataDistributionWeight;
  private final T record;

  private ShuffleRecordWrapper(T record, Map<K, Long> globalDataDistributionWeight) {
    Preconditions.checkArgument(
        record != null ^ globalDataDistributionWeight != null,
        "A ShuffleRecordWrapper has to contain exactly one of record and stats, not neither or both");
    this.globalDataDistributionWeight = globalDataDistributionWeight;
    this.record = record;
  }

  static <T, K extends Serializable> ShuffleRecordWrapper<T, K> fromRecord(T record) {
    return new ShuffleRecordWrapper<>(record, null);
  }

  static <T, K extends Serializable> ShuffleRecordWrapper<T, K> fromDistribution(
      Map<K, Long> globalDataDistributionWeight) {
    return new ShuffleRecordWrapper<>(null, globalDataDistributionWeight);
  }

  boolean hasGlobalDataDistributionWeight() {
    return globalDataDistributionWeight != null;
  }

  boolean hasRecord() {
    return record != null;
  }

  Map<K, Long> globalDataDistributionWeight() {
    return globalDataDistributionWeight;
  }

  T record() {
    return record;
  }

  @Override
  public String toString() {
    return String.format(
        "ShuffleRecordWrapper[globalDataDistributionWeight = %s, record = %s)",
        globalDataDistributionWeight, record);
  }
}
