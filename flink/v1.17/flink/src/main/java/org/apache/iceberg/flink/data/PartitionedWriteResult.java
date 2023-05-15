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
package org.apache.iceberg.flink.data;

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * A {@link WriteResult} that also contains the partition key for the data files. Data files with
 * the same partition key will be aggregated together, waiting for commit.
 */
public class PartitionedWriteResult extends WriteResult {
  private static final long serialVersionUID = 1L;

  private final PartitionKey partitionKey;

  private PartitionedWriteResult(
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      CharSequenceSet referencedDataFiles,
      PartitionKey partitionKey) {
    super(dataFiles, deleteFiles, referencedDataFiles);
    this.partitionKey = partitionKey;
  }

  public PartitionKey partitionKey() {
    return partitionKey;
  }

  public static PartitionWriteResultBuilder partitionWriteResultBuilder() {
    return new PartitionWriteResultBuilder();
  }

  public static class PartitionWriteResultBuilder {
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;
    private final CharSequenceSet referencedDataFiles;
    private PartitionKey partitionKey;

    private PartitionWriteResultBuilder() {
      this.dataFiles = Lists.newArrayList();
      this.deleteFiles = Lists.newArrayList();
      this.referencedDataFiles = CharSequenceSet.empty();
    }

    public PartitionWriteResultBuilder add(PartitionedWriteResult result) {
      addDataFiles(result.dataFiles());
      addDeleteFiles(result.deleteFiles());
      addReferencedDataFiles(result.referencedDataFiles());
      partitionKey(result.partitionKey());
      return this;
    }

    public PartitionWriteResultBuilder addAll(Iterable<PartitionedWriteResult> results) {
      results.forEach(this::add);
      return this;
    }

    public PartitionWriteResultBuilder addDataFiles(DataFile... files) {
      Collections.addAll(dataFiles, files);
      return this;
    }

    public PartitionWriteResultBuilder addDataFiles(Iterable<DataFile> files) {
      Iterables.addAll(dataFiles, files);
      return this;
    }

    public PartitionWriteResultBuilder addDeleteFiles(DeleteFile... files) {
      Collections.addAll(deleteFiles, files);
      return this;
    }

    public PartitionWriteResultBuilder addDeleteFiles(Iterable<DeleteFile> files) {
      Iterables.addAll(deleteFiles, files);
      return this;
    }

    public PartitionWriteResultBuilder addReferencedDataFiles(CharSequence... files) {
      Collections.addAll(referencedDataFiles, files);
      return this;
    }

    public PartitionWriteResultBuilder addReferencedDataFiles(Iterable<CharSequence> files) {
      Iterables.addAll(referencedDataFiles, files);
      return this;
    }

    public PartitionWriteResultBuilder partitionKey(PartitionKey newPartitionKey) {
      this.partitionKey = newPartitionKey;
      return this;
    }

    public PartitionedWriteResult build() {
      return new PartitionedWriteResult(dataFiles, deleteFiles, referencedDataFiles, partitionKey);
    }
  }
}
