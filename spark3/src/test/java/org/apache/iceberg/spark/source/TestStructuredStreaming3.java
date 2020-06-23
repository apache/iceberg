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

package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import scala.Option;
import scala.collection.JavaConversions;

public class TestStructuredStreaming3 extends TestStructuredStreaming {
  @Override
  protected <T> MemoryStream<T> newMemoryStream(int id, SQLContext sqlContext, Encoder<T> encoder) {
    return new MemoryStream<>(id, sqlContext, Option.empty(), encoder);
  }

  @Override
  protected <T> void send(List<T> records, MemoryStream<T> stream) {
    stream.addData(JavaConversions.asScalaBuffer(records));
  }
}
