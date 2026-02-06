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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;

public class TestInternalRecordWrapper extends RecordWrapperTestBase {
  @Override
  protected void generateAndValidate(Schema schema, AssertMethod assertMethod) {
    Record record = RandomGenericData.generate(schema, 1, 101L).iterator().next();
    StructLike wrapped = new InternalRecordWrapper(schema.asStruct()).wrap(record);

    for (int i = 0; i < schema.columns().size(); i++) {
      Object val = wrapped.get(i, Object.class);
      if (val != null) {
        assertThat(val).isInstanceOf(schema.columns().get(i).type().typeId().javaClass());
      }
    }
  }
}
