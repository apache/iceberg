
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

package com.netflix.iceberg.transforms;

import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.types.Types;
import org.junit.Test;

import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

public class TestUnknownTransform {

    @Test
    public void testUnknownTransform() {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "before", Types.DateType.get()),
                required(3, "after", Types.DateType.get()));

        TestHelpers.assertThrows("Should complain about unknown transfer",
                IllegalArgumentException.class, "Transform not supported: unknown:date:unknown_transform",
                () -> PartitionSpec.builderFor(schema)
                        .day("before")
                        .add(3, "after_unknown_transform", "unknown_transform")
                        .build());
    }
}
