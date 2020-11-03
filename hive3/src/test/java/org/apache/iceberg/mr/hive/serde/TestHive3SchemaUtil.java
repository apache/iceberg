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

package org.apache.iceberg.mr.hive.serde;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mr.hive.TestHiveSchemaUtil;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHive3SchemaUtil extends TestHiveSchemaUtil {

  @Override
  protected List<FieldSchema> getFieldsWithEveryPrimitiveType() {
    List<FieldSchema> fields = super.getFieldsWithEveryPrimitiveType();
    fields.add(new FieldSchema("c_timestamp_tz", serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, ""));
    return fields;
  }

  @Override
  protected Schema getSchemaWithEveryPrimitiveType() {
    List<Types.NestedField> fields = new ArrayList<>(super.getSchemaWithEveryPrimitiveType().columns());
    fields.add(optional(14, "c_timestamp_tz", Types.TimestampType.withZone()));
    return new Schema(fields);
  }
}
