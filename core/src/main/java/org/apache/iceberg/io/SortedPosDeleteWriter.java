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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;

public class SortedPosDeleteWriter<T> implements Closeable {
  private static final int RECORDS_FLUSH_NUM = 1000_000;

  private final Map<CharSequence, List<Long>> posDeletes = Maps.newHashMap();
  private final List<DeleteFile> completedFiles = Lists.newArrayList();
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileFormat format;
  private final PartitionKey partitionKey;

  private int records = 0;

  public SortedPosDeleteWriter(FileAppenderFactory<T> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileFormat format,
                               PartitionKey partitionKey) {
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.format = format;
    this.partitionKey = partitionKey;
  }

  public void delete(CharSequence path, long pos) {
    delete(path, pos, null);
  }

  public void delete(CharSequence path, long pos, T row) {
    // TODO support non-null row in future.
    Preconditions.checkArgument(row == null, "Does not support non-null row in pos-delete now.");

    posDeletes.compute(path, (k, v) -> {
      if (v == null) {
        return Lists.newArrayList(pos);
      } else {
        v.add(pos);
        return v;
      }
    });

    records += 1;
    if (records >= RECORDS_FLUSH_NUM) {
      flush();
      records = 0;
    }
  }

  public List<DeleteFile> complete() {
    flush();

    return completedFiles;
  }


  @Override
  public void close() throws IOException {
    flush();
  }

  private void flush() {
    if (posDeletes.isEmpty()) {
      return;
    }

    EncryptedOutputFile outputFile;
    if (partitionKey == null) {
      outputFile = fileFactory.newOutputFile();
    } else {
      outputFile = fileFactory.newOutputFile(partitionKey);
    }

    PositionDeleteWriter<T> writer = appenderFactory.newPosDeleteWriter(outputFile, format, partitionKey);
    try (PositionDeleteWriter<T> closeableWriter = writer) {
      CharSequence[] paths = posDeletes.keySet().toArray(new CharSequence[0]);
      Arrays.sort(paths, Comparators.charSequences());

      for (CharSequence path : paths) {
        List<Long> positions = posDeletes.get(path);
        Collections.sort(positions);

        for (Long position : positions) {
          closeableWriter.delete(path, position);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // Clear the buffered pos-deletions.
    posDeletes.clear();

    completedFiles.add(writer.toDeleteFile());
  }
}
