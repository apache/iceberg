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
package org.apache.iceberg.io.datafile;

import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.util.ArrayUtil;

/**
 * Base implementation for the {@link EqualityDeleteWriterBuilder} which handles the common
 * attributes for the builders. FileFormat implementations should extend this for creating their own
 * equality delete writer builders. Uses an embedded {@link AppenderBuilder} to actually write the
 * records.
 *
 * @param <T> the type of the builder for chaining
 */
public abstract class EqualityDeleteWriterBuilderBase<T extends EqualityDeleteWriterBuilderBase<T>>
    extends DeleteWriterBuilderBase<T> implements EqualityDeleteWriterBuilder<T> {
  private int[] equalityFieldIds = null;

  protected EqualityDeleteWriterBuilderBase(AppenderBuilder<?> appenderBuilder, FileFormat format) {
    super(appenderBuilder, format);
  }

  @Override
  public T equalityFieldIds(List<Integer> fieldIds) {
    this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
    return (T) this;
  }

  @Override
  public T equalityFieldIds(int... fieldIds) {
    this.equalityFieldIds = fieldIds;
    return (T) this;
  }

  protected int[] equalityFieldIds() {
    return equalityFieldIds;
  }
}
