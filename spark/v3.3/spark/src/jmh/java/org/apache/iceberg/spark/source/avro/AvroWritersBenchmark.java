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
package org.apache.iceberg.spark.source.avro;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.spark.source.WritersBenchmark;

/**
 * A benchmark that evaluates the performance of various Iceberg writers for Avro data.
 *
 * <p>To run this benchmark for spark-3.3: <code>
 *   ./gradlew -DsparkVersions=3.3 :iceberg-spark:iceberg-spark-3.3_2.12:jmh
 *       -PjmhIncludeRegex=AvroWritersBenchmark
 *       -PjmhOutputPath=benchmark/avro-writers-benchmark-result.txt
 * </code>
 */
public class AvroWritersBenchmark extends WritersBenchmark {

  @Override
  protected FileFormat fileFormat() {
    return FileFormat.AVRO;
  }
}
