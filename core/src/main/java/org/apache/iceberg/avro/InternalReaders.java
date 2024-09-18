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
package org.apache.iceberg.avro;

import java.util.List;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

class InternalReaders {
  private InternalReaders() {}

  static ValueReader<? extends Record> struct(
      Types.StructType struct, List<Pair<Integer, ValueReader<?>>> readPlan) {
    return new RecordReader(readPlan, struct);
  }

  static <S extends StructLike> ValueReader<S> struct(
      Types.StructType struct, Class<S> structClass, List<Pair<Integer, ValueReader<?>>> readPlan) {
    return new PlannedStructLikeReader<>(readPlan, struct, structClass);
  }

  private static class PlannedStructLikeReader<S extends StructLike>
      extends ValueReaders.PlannedStructReader<S> {
    private final Types.StructType structType;
    private final Class<S> structClass;
    private final DynConstructors.Ctor<S> ctor;

    private PlannedStructLikeReader(
        List<Pair<Integer, ValueReader<?>>> readPlan,
        Types.StructType structType,
        Class<S> structClass) {
      super(readPlan);
      this.structType = structType;
      this.structClass = structClass;
      this.ctor =
          DynConstructors.builder(StructLike.class)
              .hiddenImpl(structClass, Types.StructType.class)
              .hiddenImpl(structClass)
              .build();
    }

    @Override
    protected S reuseOrCreate(Object reuse) {
      if (structClass.isInstance(reuse)) {
        return structClass.cast(reuse);
      } else {
        return ctor.newInstance(structType);
      }
    }

    @Override
    protected Object get(S struct, int pos) {
      return struct.get(pos, Object.class);
    }

    @Override
    protected void set(S struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }

  private static class RecordReader extends ValueReaders.PlannedStructReader<GenericRecord> {
    private final Types.StructType structType;

    private RecordReader(List<Pair<Integer, ValueReader<?>>> readPlan, Types.StructType structType) {
      super(readPlan);
      this.structType = structType;
    }

    @Override
    protected GenericRecord reuseOrCreate(Object reuse) {
      if (reuse instanceof GenericRecord) {
        return (GenericRecord) reuse;
      } else {
        return GenericRecord.create(structType);
      }
    }

    @Override
    protected Object get(GenericRecord struct, int pos) {
      return struct.get(pos);
    }

    @Override
    protected void set(GenericRecord struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }
}
