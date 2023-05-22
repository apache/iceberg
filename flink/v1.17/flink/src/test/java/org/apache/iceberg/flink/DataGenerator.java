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
package org.apache.iceberg.flink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;

/**
 * This interface defines test data generator. Different implementations for primitive and complex
 * nested fields are defined in {@link DataGenerators}.
 */
public interface DataGenerator {
  Schema icebergSchema();

  RowType flinkRowType();

  org.apache.avro.Schema avroSchema();

  GenericRecord generateIcebergGenericRecord();

  GenericRowData generateFlinkRowData();

  org.apache.avro.generic.GenericRecord generateAvroGenericRecord();
}
