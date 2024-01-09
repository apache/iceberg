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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFlinkPackage {

  /** This unit test would need to be adjusted as new Flink version is supported. */
  @AfterClass
  public static void cleanup() {
    FlinkPackage.setVersionDetector(new FlinkVersionDetector());
  }

  @Test
  public void testVersion() {
    Assert.assertEquals("1.18.0", FlinkPackage.version());
  }

  @Test
  public void testDefaultVersion() {
    // It's difficult to reproduce a reflection error in a unit test, so we just inject a mocked
    // fault to test
    // the default logic
    FlinkVersionDetector detectorWithReflectionError = Mockito.spy(FlinkVersionDetector.class);
    Mockito.when(detectorWithReflectionError.getVersionFromJar()).thenThrow(RuntimeException.class);
    FlinkPackage.setVersionDetector(detectorWithReflectionError);
    Assert.assertEquals("1.18.x", FlinkPackage.version());
  }
}
