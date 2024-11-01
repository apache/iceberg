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

import java.util.Collection;
import java.util.List;
import java.util.function.LongConsumer;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BitmapPositionDeleteIndex implements PositionDeleteIndex {
  private final RoaringPositionBitmap bitmap;
  private final List<DeleteFile> deleteFiles;

  BitmapPositionDeleteIndex() {
    this.bitmap = new RoaringPositionBitmap();
    this.deleteFiles = Lists.newArrayList();
  }

  BitmapPositionDeleteIndex(Collection<DeleteFile> deleteFiles) {
    this.bitmap = new RoaringPositionBitmap();
    this.deleteFiles = Lists.newArrayList(deleteFiles);
  }

  BitmapPositionDeleteIndex(DeleteFile deleteFile) {
    this.bitmap = new RoaringPositionBitmap();
    this.deleteFiles = deleteFile != null ? Lists.newArrayList(deleteFile) : Lists.newArrayList();
  }

  void merge(BitmapPositionDeleteIndex that) {
    bitmap.setAll(that.bitmap);
    deleteFiles.addAll(that.deleteFiles);
  }

  @Override
  public void delete(long position) {
    bitmap.set(position);
  }

  @Override
  public void delete(long posStart, long posEnd) {
    bitmap.setRange(posStart, posEnd);
  }

  @Override
  public void merge(PositionDeleteIndex that) {
    if (that instanceof BitmapPositionDeleteIndex) {
      merge((BitmapPositionDeleteIndex) that);
    } else {
      that.forEach(this::delete);
      deleteFiles.addAll(that.deleteFiles());
    }
  }

  @Override
  public boolean isDeleted(long position) {
    return bitmap.contains(position);
  }

  @Override
  public boolean isEmpty() {
    return bitmap.isEmpty();
  }

  @Override
  public void forEach(LongConsumer consumer) {
    bitmap.forEach(consumer);
  }

  @Override
  public Collection<DeleteFile> deleteFiles() {
    return deleteFiles;
  }
}
