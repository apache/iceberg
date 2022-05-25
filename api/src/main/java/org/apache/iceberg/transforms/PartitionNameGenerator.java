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

package org.apache.iceberg.transforms;

public class PartitionNameGenerator implements PartitionSpecVisitor<String> {
  private static final PartitionNameGenerator INSTANCE = new PartitionNameGenerator();

  private PartitionNameGenerator() {
  }

  public static PartitionNameGenerator getInstance() {
    return INSTANCE;
  }

  @Override
  public String identity(int fieldId, String sourceName, int sourceId) {
    return sourceName;
  }

  @Override
  public String identity(String sourceName, int sourceId) {
    return identity(-1, sourceName, sourceId);
  }

  @Override
  public String bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
    return sourceName + "_bucket_" + numBuckets;
  }

  @Override
  public String bucket(String sourceName, int sourceId, int numBuckets) {
    return bucket(-1, sourceName, sourceId, numBuckets);
  }

  @Override
  public String truncate(int fieldId, String sourceName, int sourceId, int width) {
    return sourceName + "_trunc_" + width;
  }

  @Override
  public String truncate(String sourceName, int sourceId, int width) {
    return truncate(-1, sourceName, sourceId, width);
  }

  @Override
  public String year(int fieldId, String sourceName, int sourceId) {
    return sourceName + "_year";
  }

  @Override
  public String year(String sourceName, int sourceId) {
    return year(-1, sourceName, sourceId);
  }

  @Override
  public String month(int fieldId, String sourceName, int sourceId) {
    return sourceName + "_month";
  }

  @Override
  public String month(String sourceName, int sourceId) {
    return month(-1, sourceName, sourceId);
  }

  @Override
  public String day(int fieldId, String sourceName, int sourceId) {
    return sourceName + "_day";
  }

  @Override
  public String day(String sourceName, int sourceId) {
    return day(-1, sourceName, sourceId);
  }

  @Override
  public String hour(int fieldId, String sourceName, int sourceId) {
    return sourceName + "_hour";
  }

  @Override
  public String hour(String sourceName, int sourceId) {
    return hour(-1, sourceName, sourceId);
  }

  @Override
  public String alwaysNull(int fieldId, String sourceName, int sourceId) {
    return sourceName + "_null";
  }

  @Override
  public String alwaysNull(String sourceName, int sourceId) {
    return alwaysNull(-1, sourceName, sourceId);
  }
}
