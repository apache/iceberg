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

package org.apache.iceberg.data.orc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.OrcRowReader;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericOrcReader extends BaseOrcReader<Record> {
  private final OrcValueReader<?> reader;

  private GenericOrcReader(org.apache.iceberg.Schema expectedSchema,
                           TypeDescription readOrcSchema,
                           Map<Integer, ?> idToConstant) {
    this.reader = OrcSchemaWithTypeVisitor.visit(expectedSchema, readOrcSchema, new ReadBuilder(idToConstant));
  }

  public static OrcRowReader<Record> buildReader(Schema expectedSchema, TypeDescription fileSchema) {
    return buildReader(expectedSchema, fileSchema, Collections.emptyMap());
  }

  public static OrcRowReader<Record> buildReader(
      Schema expectedSchema, TypeDescription fileSchema, Map<Integer, ?> idToConstant) {
    return new GenericOrcReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  public Record read(VectorizedRowBatch batch, int row) {
    return (Record) reader.read(new StructColumnVector(batch.size, batch.cols), row);
  }

  @Override
  protected OrcValueReader<Record> createRecordReader(List<OrcValueReader<?>> fields,
                                                      Types.StructType expected,
                                                      Map<Integer, ?> idToConstant) {
    return GenericOrcReaders.struct(fields, expected, idToConstant);
  }
}
