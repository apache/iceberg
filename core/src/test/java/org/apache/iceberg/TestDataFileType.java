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

package org.apache.iceberg;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDataFileType extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestDataFileType(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testMultipleDeletes() {
    DataFiles.Builder builder = new DataFiles.Builder();
    DataFile fileA = builder.copy(FILE_A).withFileType(DataFile.FileType.POSITION_DELETE_FILE).build();
    DataFile fileB = builder.copy(FILE_B).withFileType(DataFile.FileType.EQUALITY_DELETE_FILE).build();
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .appendFile(FILE_C)
        .commit();

    for (DataFile dataFile : table.currentSnapshot().addedFiles()) {
      if (dataFile.path().toString().equals("/path/to/data-a.parquet")) {
        Assert.assertEquals(dataFile.fileType(), DataFile.FileType.POSITION_DELETE_FILE);
      }
      if (dataFile.path().toString().equals("/path/to/data-b.parquet")) {
        Assert.assertEquals(dataFile.fileType(), DataFile.FileType.EQUALITY_DELETE_FILE);
      }
      if (dataFile.path().toString().equals("/path/to/data-c.parquet")) {
        Assert.assertEquals(dataFile.fileType(), DataFile.FileType.DATA_FILE);
      }
    }
  }
}
