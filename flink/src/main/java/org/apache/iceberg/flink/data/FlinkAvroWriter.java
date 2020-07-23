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

package org.apache.iceberg.flink.data;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.flink.types.Row;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.data.avro.DataWriter;

public class FlinkAvroWriter extends DataWriter<Row> {
  public FlinkAvroWriter(Schema schema) {
    super(schema);
  }

  @Override
  protected ValueWriter<?> createStructWriter(List<ValueWriter<?>> fields) {
    return new RowWriter(fields);
  }

  private static class RowWriter extends ValueWriters.StructWriter<Row> {
    private RowWriter(List<ValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Row struct, int pos) {
      return struct.getField(pos);
    }
  }
}
