/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.netflix.iceberg.types.TypeUtil.getProjectedIds;

public class ProjectionDatumReader<D> implements DatumReader<D> {
  private final Function<Schema, DatumReader<?>> getReader;
  private final com.netflix.iceberg.Schema expectedSchema;
  private final Map<String, String> renames;
  private Schema readSchema = null;
  private Schema fileSchema = null;
  private DatumReader<D> wrapped = null;

  public ProjectionDatumReader(Function<Schema, DatumReader<?>> getReader,
                               com.netflix.iceberg.Schema expectedSchema,
                               Map<String, String> renames) {
    this.getReader = getReader;
    this.expectedSchema = expectedSchema;
    this.renames = renames;
  }

  @Override
  public void setSchema(Schema fileSchema) {
    this.fileSchema = fileSchema;
    Set<Integer> projectedIds = getProjectedIds(expectedSchema);
    Schema prunedSchema = AvroSchemaUtil.pruneColumns(fileSchema, projectedIds);
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
