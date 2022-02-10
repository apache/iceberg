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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TestFileWriterFactory;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestGenericFileWriterFactory extends TestFileWriterFactory<Record> {
  public TestGenericFileWriterFactory(FileFormat fileFormat, boolean partitioned) {
    super(fileFormat, partitioned);
  }

  @Override
  protected FileWriterFactory<Record> newWriterFactory(
      Schema dataSchema, List<Integer> equalityFieldIds,
      Schema equalityDeleteRowSchema,
      Schema positionDeleteRowSchema) {
    return GenericFileWriterFactory.builderFor(table)
        .dataSchema(table.schema())
        .dataFileFormat(format())
        .deleteFileFormat(format())
        .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
        .equalityDeleteRowSchema(equalityDeleteRowSchema)
        .positionDeleteRowSchema(positionDeleteRowSchema)
        .build();
  }

  @Override
  protected Record toRow(Integer id, String data) {
    StructType struct = StructType.of(
        required(1, "id", IntegerType.get()),
        optional(2, "data", StringType.get())
    );
    Record record = GenericRecord.create(struct);

    record.set(0, id);
    record.set(1, data);

    return record;
  }

  @Override
  protected StructLikeSet toSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    StructType structType = table.schema().asStruct();

    for (Record row : records) {
      InternalRecordWrapper wrapper = new InternalRecordWrapper(structType);
      set.add(wrapper.wrap(row));
    }
    return set;
  }
}
