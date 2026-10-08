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
package org.apache.iceberg.data;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.formats.StitchLayout;
import org.apache.iceberg.formats.Stitcher;
import org.apache.iceberg.formats.StitcherBuilder;
import org.apache.iceberg.formats.StitcherRegistry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class GenericStitchers {
  private GenericStitchers() {}

  public static void register() {
    StitcherRegistry.register(new RecordStitcherBuilder());
  }

  private static class RecordStitcherBuilder implements StitcherBuilder<Record> {
    @Override
    public Class<Record> type() {
      return Record.class;
    }

    @Override
    public Stitcher<Record> build(Schema projection, List<Schema> verticalSplits) {
      StitchLayout layout = StitchLayout.of(projection, verticalSplits);
      Types.StructType struct = projection.asStruct();
      Map<String, Integer> positions = Maps.newHashMap();
      List<Types.NestedField> fields = struct.fields();
      for (int pos = 0; pos < fields.size(); pos += 1) {
        positions.put(fields.get(pos).name(), pos);
      }

      return (records, count) ->
          new StitchedRecord(struct, positions, layout, records.toArray(new Record[0]));
    }
  }

  private static class StitchedRecord implements Record {
    private final Types.StructType struct;
    private final Map<String, Integer> positions;
    private final StitchLayout layout;
    private final Record[] parts;

    private StitchedRecord(
        Types.StructType struct,
        Map<String, Integer> positions,
        StitchLayout layout,
        Record[] parts) {
      this.struct = struct;
      this.positions = positions;
      this.layout = layout;
      this.parts = parts;
    }

    private Record part(int pos) {
      return parts[layout.split(pos)];
    }

    private int ordinal(int pos) {
      return layout.ordinal(pos);
    }

    @Override
    public Types.StructType struct() {
      return struct;
    }

    @Override
    public Object getField(String name) {
      Integer pos = positions.get(name);
      if (pos != null) {
        return get(pos);
      }

      return null;
    }

    @Override
    public void setField(String name, Object value) {
      Integer pos = positions.get(name);
      Preconditions.checkArgument(pos != null, "Cannot set unknown field named: %s", name);
      set(pos, value);
    }

    @Override
    public int size() {
      return layout.size();
    }

    @Override
    public Object get(int pos) {
      return part(pos).get(ordinal(pos));
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return part(pos).get(ordinal(pos), javaClass);
    }

    @Override
    public <T> void set(int pos, T value) {
      part(pos).set(ordinal(pos), value);
    }

    @Override
    public Record copy() {
      return toGenericRecord().copy();
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
      return toGenericRecord().copy(overwriteValues);
    }

    private GenericRecord toGenericRecord() {
      GenericRecord record = GenericRecord.create(struct);
      for (int pos = 0; pos < size(); pos += 1) {
        record.set(pos, get(pos));
      }

      return record;
    }
  }
}
