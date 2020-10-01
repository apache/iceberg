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

package org.apache.iceberg.spark.source;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.LocationProvider;

/**
 * Custom location provider for testing in {@link TestDataFrameWrites}
 */
public class TestLocationProvider implements LocationProvider {
  private final String truePath;
  private final String falsePath;

  public static final String TRUE_PATH = "true.path";
  public static final String FALSE_PATH = "false.path";

  public TestLocationProvider(String location, Map<String, String> properties) {
    this.truePath = properties.get(TRUE_PATH);
    this.falsePath = properties.get(FALSE_PATH);
  }

  @Override
  public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
    Boolean bVal = (Boolean) partitionData.get(0, spec.javaClasses()[0]);
    String localizedLocation = bVal ? this.truePath : this.falsePath;
    return String.format("%s/%s/%s", localizedLocation, spec.partitionToPath(partitionData), filename);
  }

  @Override
  public String newDataLocation(String filename) {
    throw new RuntimeException("Don't expect this in test");
  }
}
