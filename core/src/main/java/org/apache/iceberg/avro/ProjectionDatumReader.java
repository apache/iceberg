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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;

public class ProjectionDatumReader<D> implements DatumReader<D>, SupportsRowPosition {
  private final Function<Schema, DatumReader<?>> getReader;
  private final org.apache.iceberg.Schema expectedSchema;
  private final Map<String, String> renames;
  private NameMapping nameMapping;
  private Schema readSchema = null;
  private Schema fileSchema = null;
  private DatumReader<D> wrapped = null;

  public ProjectionDatumReader(
      Function<Schema, DatumReader<?>> getReader,
      org.apache.iceberg.Schema expectedSchema,
      Map<String, String> renames,
      NameMapping nameMapping) {
    this.getReader = getReader;
    this.expectedSchema = expectedSchema;
    this.renames = renames;
    this.nameMapping = nameMapping;
  }

  @Override
  public void setRowPositionSupplier(Supplier<Long> posSupplier) {
    if (wrapped instanceof SupportsRowPosition) {
      ((SupportsRowPosition) wrapped).setRowPositionSupplier(posSupplier);
    }
  }

  @Override
  public void setSchema(Schema newFileSchema) {
    this.fileSchema = newFileSchema;
    if (nameMapping == null && !AvroSchemaUtil.hasIds(fileSchema)) {
      nameMapping = MappingUtil.create(expectedSchema);
    }
    Set<Integer> projectedIds = TypeUtil.getProjectedIds(expectedSchema);
    Schema schemaWithIds = AvroSchemaUtil.applyNameMapping(newFileSchema, nameMapping);
    Schema prunedSchema = AvroSchemaUtil.pruneColumns(schemaWithIds, projectedIds);
    this.readSchema = AvroSchemaUtil.buildAvroProjection(prunedSchema, expectedSchema, renames);
    this.wrapped = newDatumReader();
  }

  @Override
  public D read(D reuse, Decoder in) throws IOException {
    return wrapped.read(reuse, in);
  }

  @SuppressWarnings("unchecked")
  private DatumReader<D> newDatumReader() {
    DatumReader<D> reader = (DatumReader<D>) getReader.apply(readSchema);
    reader.setSchema(fileSchema);
    return reader;
  }
}
