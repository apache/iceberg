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

import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionedWriter extends BaseWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedWriter.class);

  private final PartitionKey key;
  private final Set<PartitionKey> completedPartitions = Sets.newHashSet();

  PartitionedWriter(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                    OutputFileFactory fileFactory, FileIO io, long targetFileSize, Schema writeSchema) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.key = new PartitionKey(spec, writeSchema);
  }

  @Override
  public void write(InternalRow row) throws IOException {
    key.partition(row);

    PartitionKey currentKey = getCurrentKey();
    if (!key.equals(currentKey)) {
      closeCurrent();
      completedPartitions.add(currentKey);

      if (completedPartitions.contains(key)) {
        // if rows are not correctly grouped, detect and fail the write
        PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
        LOG.warn("Duplicate key: {} == {}", existingKey, key);
        throw new IllegalStateException("Already closed files for partition: " + key.toPath());
      }

      setCurrentKey(key.copy());
      openCurrent();
    }

    writeInternal(row);
  }
}
