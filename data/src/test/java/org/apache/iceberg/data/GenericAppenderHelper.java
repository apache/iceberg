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
package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

/** Helper for appending {@link DataFile} to a table or appending {@link Record}s to a table. */
public class GenericAppenderHelper {

  private static final String ORC_CONFIG_PREFIX = "^orc.*";
  private static final String PARQUET_CONFIG_PATTERN = ".*parquet.*";

  private final Table table;
  private final FileFormat fileFormat;
  private final Path tmp;
  private final Configuration conf;

  @Deprecated()
  public GenericAppenderHelper(
      Table table, FileFormat fileFormat, TemporaryFolder tmp, Configuration conf)
      throws IOException {
    this.table = table;
    this.fileFormat = fileFormat;
    this.tmp = tmp.newFolder().toPath();
    this.conf = conf;
  }

  public GenericAppenderHelper(Table table, FileFormat fileFormat, Path tmp, Configuration conf) {
    this.table = table;
    this.fileFormat = fileFormat;
    this.tmp = tmp;
    this.conf = conf;
  }

  public GenericAppenderHelper(Table table, FileFormat fileFormat, TemporaryFolder tmp)
      throws IOException {
    this(table, fileFormat, tmp, null);
  }

  public void appendToTable(String branch, DataFile... dataFiles) {
    Preconditions.checkNotNull(table, "table not set");

    AppendFiles append =
        table.newAppend().toBranch(branch != null ? branch : SnapshotRef.MAIN_BRANCH);

    for (DataFile dataFile : dataFiles) {
      append = append.appendFile(dataFile);
    }

    append.commit();
  }

  public void appendToTable(DataFile... dataFiles) {
    appendToTable(null, dataFiles);
  }

  public void appendToTable(List<Record> records) throws IOException {
    appendToTable(null, null, records);
  }

  public void appendToTable(String branch, List<Record> records) throws IOException {
    appendToTable(null, branch, records);
  }

  public void appendToTable(StructLike partition, String branch, List<Record> records)
      throws IOException {
    appendToTable(branch, writeFile(partition, records));
  }

  public void appendToTable(StructLike partition, List<Record> records) throws IOException {
    appendToTable(writeFile(partition, records));
  }

  public DataFile writeFile(List<Record> records) throws IOException {
    Preconditions.checkNotNull(table, "table not set");
    File parent = java.nio.file.Files.createTempDirectory(tmp, null).toFile();
    File tempFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp", parent);
    Assert.assertTrue(tempFile.delete());
    return appendToLocalFile(table, tempFile, fileFormat, null, records, conf);
  }

  public DataFile writeFile(StructLike partition, List<Record> records) throws IOException {
    Preconditions.checkNotNull(table, "table not set");
    File parent = java.nio.file.Files.createTempDirectory(tmp, null).toFile();
    File tempFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp", parent);
    Assert.assertTrue(tempFile.delete());
    return appendToLocalFile(table, tempFile, fileFormat, partition, records, conf);
  }

  private static DataFile appendToLocalFile(
      Table table,
      File file,
      FileFormat format,
      StructLike partition,
      List<Record> records,
      Configuration conf)
      throws IOException {
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());

    // Push down ORC related settings to appender if there are any
    if (FileFormat.ORC.equals(format) && conf != null) {
      appenderFactory.setAll(conf.getValByRegex(ORC_CONFIG_PREFIX));
    }

    if (FileFormat.PARQUET.equals(format) && conf != null) {
      appenderFactory.setAll(conf.getValByRegex(PARQUET_CONFIG_PATTERN));
    }

    FileAppender<Record> appender = appenderFactory.newAppender(Files.localOutput(file), format);
    try (FileAppender<Record> fileAppender = appender) {
      fileAppender.addAll(records);
    }

    return DataFiles.builder(table.spec())
        .withRecordCount(records.size())
        .withFileSizeInBytes(file.length())
        .withPath(Files.localInput(file).location())
        .withMetrics(appender.metrics())
        .withFormat(format)
        .withPartition(partition)
        .withSplitOffsets(appender.splitOffsets())
        .build();
  }
}
