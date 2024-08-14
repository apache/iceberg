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
package org.apache.iceberg.flink.source.reader;

import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
public class ReaderFunctionAdaptor<T> implements Reader<T> {
  private final ReaderFunction<T> readerFunction;
  private final TypeInformation<T> outputTypeInfo;

  public ReaderFunctionAdaptor(
      ReaderFunction<T> readerFunction, @Nullable TypeInformation<T> outputTypeInfo) {
    this.readerFunction = readerFunction;
    this.outputTypeInfo = outputTypeInfo;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> open(
      IcebergSourceSplit split) {
    return readerFunction.apply(split);
  }

  @Override
  public TypeInformation<T> outputTypeInfo() {
    Preconditions.checkState(outputTypeInfo != null, "Output type info is not provided");
    return outputTypeInfo;
  }
}
