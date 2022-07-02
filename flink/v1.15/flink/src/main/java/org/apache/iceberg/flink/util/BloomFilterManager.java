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

package org.apache.iceberg.flink.util;

import org.apache.iceberg.relocated.com.google.common.hash.BloomFilter;

public class BloomFilterManager {

  private final BloomFilter<CharSequence> filter;
  private final long bloomFilterCreateTime;

  public BloomFilterManager(BloomFilter<CharSequence> filter,
                            long bloomFilterCreateTime) {
    this.filter = filter;
    this.bloomFilterCreateTime = bloomFilterCreateTime;
  }

  public void setKeyToFilter(String key) {
    if (filter != null) {
      filter.put(key);
    }
  }

  public boolean isKeyInFilter(String key) {
    if (filter != null) {
      return filter.mightContain(key);
    }

    return false;
  }

  public BloomFilter<CharSequence> getFilter() {
    return filter;
  }

  public long getBloomFilterCreateTime() {
    return bloomFilterCreateTime;
  }
}
