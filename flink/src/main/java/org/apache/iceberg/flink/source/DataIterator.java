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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * Base class of Flink iterators.
 *
 * @param <T> is the Java class returned by this iterator whose objects contain one or more rows.
 */
abstract class DataIterator<T> implements CloseableIterator<T> {

  private final Iterator<FileScanTask> tasks;
  private final FileIO fileIo;
  private final EncryptionManager encryption;

  private CloseableIterator<T> currentIterator;

  DataIterator(CombinedScanTask task, FileIO fileIo, EncryptionManager encryption) {
    this.tasks = task.files().iterator();
    this.fileIo = fileIo;
    this.encryption = encryption;
    this.currentIterator = CloseableIterator.empty();
  }

  InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
    return encryption.decrypt(EncryptedFiles.encryptedInput(
        fileIo.newInputFile(task.file().path().toString()),
        task.file().keyMetadata()));
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    return currentIterator.next();
  }

  /**
   * Updates the current iterator field to ensure that the current Iterator
   * is not exhausted.
   */
  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && tasks.hasNext()) {
        currentIterator.close();
        currentIterator = openTaskIterator(tasks.next());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  abstract CloseableIterator<T> openTaskIterator(FileScanTask scanTask) throws IOException;

  @Override
  public void close() throws IOException {
    // close the current iterator
    this.currentIterator.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case DECIMAL: // DecimalData
        Types.DecimalType decimal = (Types.DecimalType) type;
        return DecimalData.fromBigDecimal((BigDecimal) value, decimal.precision(), decimal.scale());
      case STRING: // StringData
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          return StringData.fromBytes(utf8.getBytes(), 0, utf8.getByteLength());
        }
        return StringData.fromString(value.toString());
      case FIXED: // byte[]
        if (value instanceof byte[]) {
          return value;
        } else if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case BINARY: // byte[]
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case TIME: // int mills instead of long
        return (int) ((Long) value / 1000);
      case TIMESTAMP: // TimestampData
        return TimestampData.fromLocalDateTime(DateTimeUtil.timestampFromMicros((Long) value));
      default:
    }
    return value;
  }
}
