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

package org.apache.iceberg.io;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class TestLocationProvider {

  @Test
  public void newDataLocationWithOption() {
    LocationProvider locationProvider = new SimpleRegionalLocationProvider();

    Map<String, String> option1 = ImmutableMap.of("region", "region1");
    Assert.assertEquals("location1/f1", locationProvider.newDataLocation("f1", option1));

    Map<String, String> option2 = ImmutableMap.of("region", "region2");
    Assert.assertEquals("location2/f2", locationProvider.newDataLocation("f2", option2));
  }

  public static class SimpleRegionalLocationProvider implements LocationProvider {

    private static Map<String, String> regionalLocations = ImmutableMap.of(
        "region1", "location1/",
        "region2", "location2/"
    );

    @Override
    public String newDataLocation(String filename) {
      return null;
    }

    @Override
    public String newDataLocation(String filename, Map<String, String> options) {
      return regionalLocations.get(options.get("region")) + filename;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return null;
    }

    @Override
    public String newDataLocation(
        PartitionSpec spec,
        StructLike partitionData,
        String filename,
        Map<String, String> options) {
      return null;
    }
  }
}
