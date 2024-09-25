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

import java.util.function.LongConsumer;
import org.roaringbitmap.longlong.Roaring64Bitmap;

class BitmapPositionDeleteIndex implements PositionDeleteIndex {
  private final Roaring64Bitmap roaring64Bitmap;

  BitmapPositionDeleteIndex() {
    roaring64Bitmap = new Roaring64Bitmap();
  }

  void merge(BitmapPositionDeleteIndex that) {
    roaring64Bitmap.or(that.roaring64Bitmap);
  }

  @Override
  public void delete(long position) {
    roaring64Bitmap.add(position);
  }

  @Override
  public void delete(long posStart, long posEnd) {
    roaring64Bitmap.addRange(posStart, posEnd);
  }

  @Override
  public boolean isDeleted(long position) {
    return roaring64Bitmap.contains(position);
  }

  @Override
  public boolean isEmpty() {
    return roaring64Bitmap.isEmpty();
  }

  @Override
  public void forEach(LongConsumer consumer) {
    roaring64Bitmap.forEach(consumer::accept);
  }
}
