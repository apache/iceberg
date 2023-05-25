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
package org.apache.iceberg.deletes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.CharSequenceWrapper;
import org.roaringbitmap.longlong.PeekableLongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * A position delete writer that is capable of handling unordered deletes without rows.
 *
 * <p>This writer keeps an in-memory bitmap of deleted positions per each seen data file and flushes
 * the result into a file when closed. This enables writing position delete files when the incoming
 * records are not ordered by file and position as required by the spec. If the incoming deletes are
 * ordered by an external process, use {@link PositionDeleteWriter} instead.
 *
 * <p>Note this writer stores only positions. It does not store deleted records.
 */
public class SortingPositionOnlyDeleteWriter<T>
    implements FileWriter<PositionDelete<T>, DeleteWriteResult> {

  private final FileWriter<PositionDelete<T>, DeleteWriteResult> writer;
  private final Map<CharSequenceWrapper, Roaring64Bitmap> positionsByPath;
  private final CharSequenceWrapper pathWrapper;
  private DeleteWriteResult result = null;

  public SortingPositionOnlyDeleteWriter(FileWriter<PositionDelete<T>, DeleteWriteResult> writer) {
    this.writer = writer;
    this.positionsByPath = Maps.newHashMap();
    this.pathWrapper = CharSequenceWrapper.wrap(null);
  }

  @Override
  public void write(PositionDelete<T> positionDelete) {
    CharSequence path = positionDelete.path();
    long position = positionDelete.pos();
    Roaring64Bitmap positions = positionsByPath.get(pathWrapper.set(path));
    if (positions != null) {
      positions.add(position);
    } else {
      positions = new Roaring64Bitmap();
      positions.add(position);
      positionsByPath.put(CharSequenceWrapper.wrap(path), positions);
    }
  }

  @Override
  public long length() {
    return writer.length();
  }

  @Override
  public DeleteWriteResult result() {
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      this.result = writeDeletes();
    }
  }

  private DeleteWriteResult writeDeletes() throws IOException {
    try {
      PositionDelete<T> positionDelete = PositionDelete.create();
      for (CharSequenceWrapper path : sortedPaths()) {
        // the iterator provides values in ascending sorted order
        PeekableLongIterator positions = positionsByPath.get(path).getLongIterator();
        while (positions.hasNext()) {
          long position = positions.next();
          writer.write(positionDelete.set(path.get(), position, null /* no row */));
        }
      }
    } finally {
      writer.close();
    }

    return writer.result();
  }

  private List<CharSequenceWrapper> sortedPaths() {
    List<CharSequenceWrapper> paths = Lists.newArrayList(positionsByPath.keySet());
    paths.sort(Comparators.charSequences());
    return paths;
  }
}
