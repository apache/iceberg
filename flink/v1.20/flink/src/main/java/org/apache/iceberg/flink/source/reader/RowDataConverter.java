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

import java.io.Serializable;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;

/**
 * Convert RowData to a different output type.
 *
 * @param <T> output type
 */
public interface RowDataConverter<T>
    extends Function<RowData, T>, ResultTypeQueryable, Serializable {

  static IdentityConverter identity() {
    return new IdentityConverter();
  }

  class IdentityConverter implements RowDataConverter<RowData> {
    @Override
    public RowData apply(RowData from) {
      return from;
    }

    @Override
    public TypeInformation getProducedType() {
      return TypeInformation.of(RowData.class);
    }
  }
}
