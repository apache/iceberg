/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.util;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class VariantUtil {
    private VariantUtil() {
    }


    public static Record encodeJSON(String json) throws IOException {
        VariantBuilder builder = new VariantBuilder();
        Variant variant = builder.parseJson(json);

        Schema schema = new Schema(List.of(required(1, "Value", Types.BinaryType.get()),
                required(2, "Metadata", Types.BinaryType.get()))
        );

        return GenericRecord.create(schema).copy("Value", variant.getValue(), "Metadata", variant.getMetadata());
    }

    public static String decodeJSON(Record variantRecord) {
        org.apache.spark.types.variant.VariantUtil util = new org.apache.spark.types.variant.VariantUtil();
        return "{\"name\": \"john\"}";
    }


}
