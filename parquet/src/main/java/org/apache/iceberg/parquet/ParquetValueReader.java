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

package org.apache.iceberg.parquet;

import java.util.List;
import org.apache.parquet.column.page.PageReadStore;

public interface ParquetValueReader<T> {
  T read(T reuse);

  TripleIterator<?> column();

  List<TripleIterator<?>> columns();

  void setPageSource(PageReadStore pageStore, long rowPosition);

  interface OfBoolean extends ParquetValueReader<Boolean> {
    @Override
    default Boolean read(Boolean ignored) {
      return readBoolean();
    }

    boolean readBoolean();
  }

  interface OfInt extends ParquetValueReader<Integer> {
    @Override
    default Integer read(Integer ignored) {
      return readInteger();
    }

    int readInteger();
  }

  interface OfLong extends ParquetValueReader<Long> {
    @Override
    default Long read(Long ignored) {
      return readLong();
    }

    long readLong();
  }

  interface OfFloat extends ParquetValueReader<Float> {
    @Override
    default Float read(Float ignored) {
      return readFloat();
    }

    float readFloat();
  }

  interface OfDouble extends ParquetValueReader<Double> {
    @Override
    default Double read(Double ignored) {
      return readDouble();
    }

    double readDouble();
  }
}
