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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

class TestOrcValueReaders {
  @Test
  void rowLineagePreservesPhysicalReadersWithoutConstants() {
    Schema rowIdSchema = new Schema(MetadataColumns.ROW_ID);
    OrcValueReader<Long> rowIdReader = OrcValueReaders.constants(34L);

    TestStructReader rowIdStructReader = reader(rowIdSchema, List.of(rowIdReader), Map.of());

    assertThat(rowIdStructReader.reader(0)).isSameAs(rowIdReader);

    Schema lastUpdatedSchema = new Schema(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    OrcValueReader<Long> lastUpdatedReader = OrcValueReaders.constants(5L);

    TestStructReader lastUpdatedStructReader =
        reader(
            lastUpdatedSchema,
            List.of(lastUpdatedReader),
            Map.of(MetadataColumns.ROW_ID.fieldId(), 10L));

    assertThat(lastUpdatedStructReader.reader(0)).isSameAs(lastUpdatedReader);
  }

  @Test
  void lastUpdatedInheritsWithoutBase() {
    Schema schema = new Schema(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    TestStructReader reader =
        reader(
            schema,
            Collections.singletonList(null),
            Map.of(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), 5L));

    assertThat(reader.reader(0).read(null, 0)).isEqualTo(5L);
  }

  private static TestStructReader reader(
      Schema schema, List<OrcValueReader<?>> readers, Map<Integer, ?> idToConstant) {
    TypeDescription orcType = ORCSchemaUtil.convert(schema);
    return new TestStructReader(orcType, readers, schema.asStruct(), idToConstant);
  }

  private static class TestStructReader extends OrcValueReaders.StructReader<Object[]> {
    TestStructReader(
        TypeDescription orcType,
        List<OrcValueReader<?>> readers,
        Types.StructType struct,
        Map<Integer, ?> idToConstant) {
      super(orcType, readers, struct, idToConstant);
    }

    @Override
    protected Object[] create() {
      return new Object[2];
    }

    @Override
    protected void set(Object[] struct, int pos, Object value) {
      struct[pos] = value;
    }
  }
}
