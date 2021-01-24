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

package org.apache.iceberg.beam;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.beam.util.StringToGenericRecord;
import org.apache.iceberg.catalog.TableIdentifier;
import org.joda.time.Duration;
import org.junit.Test;

public class StreamingTest extends BaseTest {
  private static final Duration WINDOW_DURATION = Duration.standardMinutes(1);

  @Test
  public void testWriteFilesAvro() {
    runPipeline(FileFormat.AVRO);
  }

  @Test
  public void testWriteFilesParquet() {
    runPipeline(FileFormat.PARQUET);
  }

  @Test
  public void testWriteFilesOrc() {
    runPipeline(FileFormat.ORC);
  }

  public void runPipeline(FileFormat fileFormat) {
    pipeline.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(avroSchema));

    // We should see four commits in the log
    TestStream<String> stringsStream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(START_TIME)
            .addElements(event(SENTENCES.get(0), 2L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(60L)))
            .addElements(event(SENTENCES.get(1), 62L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(120L)))
            .addElements(event(SENTENCES.get(2), 122L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(180L)))
            .addElements(event(SENTENCES.get(3), 182L))
            .advanceWatermarkToInfinity();

    PCollection<GenericRecord> records = pipeline
        .apply(stringsStream)
        .setCoder(StringUtf8Coder.of())
        .apply(ParDo.of(new StringToGenericRecord(stringSchema)));

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    TableIdentifier name = TableIdentifier.of("default", "test_streaming_" + fileFormat.name());

    PCollection<GenericRecord> windowed = records.apply(Window.into(FixedWindows.of(WINDOW_DURATION)));

    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());

    IcebergIO.write(name, icebergSchema, hiveMetastoreUrl, windowed, properties);

    pipeline.run(options).waitUntilFinish();
  }
}
