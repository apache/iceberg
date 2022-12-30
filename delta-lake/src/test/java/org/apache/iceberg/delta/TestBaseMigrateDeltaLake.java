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
package org.apache.iceberg.delta;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

public class TestBaseMigrateDeltaLake {

  @Test
  public void testGetFullFilePath() {
    String fileName = "part-00000-12345678-1234-1234-1234-123456789012.parquet";
    String tableRoot = "s3://bucket/table";
    String relativeFilePath = "id=0" + File.separator + fileName;
    String absoluteFilePath = tableRoot + File.separator + relativeFilePath;
    Assert.assertEquals(
        "If the input is an absolute path, it should be returned as is",
        absoluteFilePath,
        BaseMigrateDeltaLakeTableAction.getFullFilePath(absoluteFilePath, tableRoot));
    Assert.assertEquals(
        "If the input is a relative path, it should be returned as an absolute path",
        absoluteFilePath,
        BaseMigrateDeltaLakeTableAction.getFullFilePath(relativeFilePath, tableRoot));
  }
}
