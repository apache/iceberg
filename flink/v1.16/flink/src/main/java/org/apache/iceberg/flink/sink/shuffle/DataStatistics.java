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

/**
 * Shuffle operator can help to improve data clustering based on the key.
 *
 * <p>It collects the data statistics information, sends to coordinator and gets the global data
 * distribution weight from coordinator. Then it will ingest the weight into data stream(wrap by a
 * class{@link ShuffleRecordWrapper}) and send to partitioner.
 */
interface DataStatistics<K> {
  long size();

  void put(K key);

  void merge(DataStatistics<K> other);
}
