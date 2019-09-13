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


package org.apache.iceberg.types;

import org.apache.iceberg.Schema;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;


public class TestTypeUtil {

  @Test(expected = IllegalArgumentException.class)
  public void testReassignIdsIllegalArgumentException() {
    Schema schema = new Schema(
        required(1, "a", Types.IntegerType.get()),
        required(2, "b", Types.IntegerType.get())
    );
    Schema sourceSchema = new Schema(
        required(1, "a", Types.IntegerType.get())
    );
    TypeUtil.reassignIds(schema, sourceSchema);
  }
}
