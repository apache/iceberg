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

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.types.Type;

/**
 * A Writer that consumes Iceberg's internal in-memory object model.
 *
 * <p>Iceberg's internal in-memory object model produces the types defined in {@link
 * Type.TypeID#javaClass()}.
 */
public class InternalWriter<T> implements MetricsAwareDatumWriter<T> {
  private ValueWriter<T> writer = null;

  public static <D> InternalWriter<D> create(Schema schema) {
    return new InternalWriter<>(schema);
  }

  InternalWriter(Schema schema) {
    setSchema(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema schema) {
    this.writer = (ValueWriter<T>) AvroSchemaVisitor.visit(schema, new WriteBuilder());
  }

  @Override
  public void write(T datum, Encoder out) throws IOException {
    writer.write(datum, out);
  }

  @Override
  public Stream<FieldMetrics> metrics() {
    return writer.metrics();
  }

  private static class WriteBuilder extends BaseWriteBuilder {

    @Override
    protected ValueWriter<?> createRecordWriter(List<ValueWriter<?>> fields) {
      return ValueWriters.struct(fields);
    }

    @Override
    protected ValueWriter<?> fixedWriter(int length) {
      return ValueWriters.fixedBuffers(length);
    }
  }
}
