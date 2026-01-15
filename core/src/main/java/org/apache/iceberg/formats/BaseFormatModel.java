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
package org.apache.iceberg.formats;

import java.util.Map;
import org.apache.iceberg.Schema;

public abstract class BaseFormatModel<D, S, W, R, F> implements FormatModel<D, S> {
  private final Class<? extends D> type;
  private final Class<S> schemaType;
  private final WriterFunction<W, S, F> writerFunction;
  private final ReaderFunction<R, S, F> readerFunction;
  private final boolean batchReader;

  public BaseFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      WriterFunction<W, S, F> writerFunction,
      ReaderFunction<R, S, F> readerFunction,
      boolean batchReader) {
    this.type = type;
    this.schemaType = schemaType;
    this.writerFunction = writerFunction;
    this.readerFunction = readerFunction;
    this.batchReader = batchReader;
  }

  @Override
  public Class<? extends D> type() {
    return type;
  }

  @Override
  public Class<S> schemaType() {
    return schemaType;
  }

  protected WriterFunction<W, S, F> writerFunction() {
    return writerFunction;
  }

  protected ReaderFunction<R, S, F> readerFunction() {
    return readerFunction;
  }

  protected boolean batchReader() {
    return batchReader;
  }

  @FunctionalInterface
  public interface WriterFunction<W, S, F> {
    W write(Schema icebergSchema, F fileSchema, S engineSchema);
  }

  @FunctionalInterface
  public interface ReaderFunction<R, S, F> {
    R read(Schema icebergSchema, F fileSchema, S engineSchema, Map<Integer, ?> idToConstant);
  }
}
