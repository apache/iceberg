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
package org.apache.iceberg.flink.source.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestLimitableDataIterator {
  @TempDir private static Path temporaryFolder;

  private final RowDataFileScanTaskReader reader =
      new RowDataFileScanTaskReader(
          TestFixtures.SCHEMA, TestFixtures.SCHEMA, null, true, Collections.emptyList());
  private final HadoopFileIO fileIO = new HadoopFileIO(new org.apache.hadoop.conf.Configuration());
  private final EncryptionManager encryptionManager = PlaintextEncryptionManager.instance();

  private static CombinedScanTask combinedScanTask;
  private static int totalRecords;

  @BeforeAll
  public static void beforeClass() throws Exception {
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(TestFixtures.SCHEMA);
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, temporaryFolder, FileFormat.PARQUET, appenderFactory);
    totalRecords = 3 * 2;
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, 0L, 1L, 6L, 7L})
  public void testUnlimited(long limit) {
    LimitableDataIterator<RowData> dataIterator =
        new LimitableDataIterator<>(
            reader, combinedScanTask, fileIO, encryptionManager, RecordLimiter.create(limit));

    List<RowData> result = Lists.newArrayList();
    while (dataIterator.hasNext()) {
      result.add(dataIterator.next());
    }

    if (limit <= 0 || limit > totalRecords) {
      // read all records
      assertThat(result).hasSize(totalRecords);
    } else {
      assertThat(result).hasSize((int) limit);
    }
  }
}
