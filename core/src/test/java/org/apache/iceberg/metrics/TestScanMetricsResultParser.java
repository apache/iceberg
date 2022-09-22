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
package org.apache.iceberg.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class TestScanMetricsResultParser {

  @Test
  public void nullMetrics() {
    Assertions.assertThatThrownBy(() -> ScanMetricsResultParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse scan metrics from null object");

    Assertions.assertThatThrownBy(() -> ScanMetricsResultParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");
  }

  @SuppressWarnings("MethodLength")
  @Test
  public void missingFields() {
    Assertions.assertThat(ScanMetricsResultParser.fromJson("{}"))
        .isEqualTo(ImmutableScanMetricsResult.builder().build());

    ImmutableScanMetricsResult scanMetricsResult =
        ImmutableScanMetricsResult.builder()
            .totalPlanningDuration(TimerResult.of(TimeUnit.HOURS, Duration.ofHours(10), 3L))
            .build();
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult = scanMetricsResult.withResultDataFiles(CounterResult.of(Unit.COUNT, 5L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult = scanMetricsResult.withResultDeleteFiles(CounterResult.of(Unit.COUNT, 5L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult = scanMetricsResult.withTotalDataManifests(CounterResult.of(Unit.COUNT, 5L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult =
        scanMetricsResult.withTotalDeleteManifests(CounterResult.of(Unit.COUNT, 0L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult =
        scanMetricsResult.withScannedDataManifests(CounterResult.of(Unit.COUNT, 5L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult =
        scanMetricsResult.withSkippedDataManifests(CounterResult.of(Unit.COUNT, 5L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult =
        scanMetricsResult.withTotalFileSizeInBytes(CounterResult.of(Unit.BYTES, 1069L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult =
        scanMetricsResult.withTotalDeleteFileSizeInBytes(CounterResult.of(Unit.BYTES, 1023L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069},"
                    + "\"total-delete-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1023}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult = scanMetricsResult.withSkippedDataFiles(CounterResult.of(Unit.COUNT, 23L));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069},"
                    + "\"total-delete-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1023},"
                    + "\"skipped-data-files\":{\"unit\":\"count\",\"value\":23}}"))
        .isEqualTo(scanMetricsResult);

    scanMetricsResult =
        scanMetricsResult.withResultDataFilesByFormat(
            MultiDimensionCounterResult.of(
                ScanMetrics.RESULT_DATA_FILES_BY_FORMAT,
                Unit.COUNT,
                ImmutableMap.of(FileFormat.AVRO.toString(), 11L, FileFormat.ORC.toString(), 22L)));
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":3,\"time-unit\":\"hours\",\"total-duration\":10},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-data-files-by-format\":{\"unit\":\"count\",\"counters\":[{\"counter-id\":\"AVRO\",\"value\":11},{\"counter-id\":\"ORC\",\"value\":22}]},"
                    + "\"result-avro-data-files\":{\"unit\":\"count\",\"value\":11},"
                    + "\"result-orc-data-files\":{\"unit\":\"count\",\"value\":22},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069},"
                    + "\"total-delete-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1023},"
                    + "\"skipped-data-files\":{\"unit\":\"count\",\"value\":23}}"))
        .isEqualTo(scanMetricsResult);
  }

  @Test
  public void extraFields() {
    ScanMetrics scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics
        .resultDataFilesByFormat()
        .increment(new DefaultMetricTag(FileFormat.AVRO.toString()), 51);
    scanMetrics
        .resultDataFilesByFormat()
        .increment(new DefaultMetricTag(FileFormat.ORC.toString()), 52);
    scanMetrics
        .resultDataFilesByFormat()
        .increment(new DefaultMetricTag(FileFormat.PARQUET.toString()), 53);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.skippedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);
    scanMetrics.skippedDataFiles().increment(3L);
    scanMetrics.skippedDeleteFiles().increment(3L);
    scanMetrics.scannedDeleteManifests().increment(3L);
    scanMetrics.skippedDeleteManifests().increment(3L);
    scanMetrics.indexedDeleteFiles().increment(10L);
    scanMetrics.positionalDeleteFiles().increment(6L);
    scanMetrics.equalityDeleteFiles().increment(4L);

    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(scanMetrics);
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-data-files-by-format\":{\"unit\":\"count\",\"counters\":[{\"counter-id\":\"AVRO\",\"value\":51},{\"counter-id\":\"ORC\",\"value\":52},{\"counter-id\":\"PARQUET\",\"value\":53}]},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069},"
                    + "\"total-delete-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":23},"
                    + "\"skipped-data-files\":{\"unit\":\"count\",\"value\":3},"
                    + "\"skipped-delete-files\":{\"unit\":\"count\",\"value\":3},"
                    + "\"scanned-delete-manifests\":{\"unit\":\"count\",\"value\":3},"
                    + "\"skipped-delete-manifests\":{\"unit\":\"count\",\"value\":3},"
                    + "\"indexed-delete-files\":{\"unit\":\"count\",\"value\":10},"
                    + "\"equality-delete-files\":{\"unit\":\"count\",\"value\":4},"
                    + "\"positional-delete-files\":{\"unit\":\"count\",\"value\":6},"
                    + "\"extra\": \"value\",\"extra2\":23}"))
        .isEqualTo(scanMetricsResult);
  }

  @Test
  public void invalidTimer() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timer from 'total-planning-duration': Missing field 'count'");
  }

  @Test
  public void invalidCounter() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from 'result-data-files': Missing field 'unit'");
  }

  @Test
  public void invalidMultiDimensionCounter() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"result-data-files-by-format\":{\"counters\":[{\"counter-id\":\"AVRO\",\"value\":51},{\"counter-id\":\"ORC\",\"value\":52},{\"counter-id\":\"PARQUET\",\"value\":53}]}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse counter from 'result-data-files-by-format': Missing field 'unit'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"result-data-files-by-format\":{\"unit\":\"count\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse counter from 'result-data-files-by-format': Missing field 'counters'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"result-data-files-by-format\":{\"unit\":\"count\",\"counters\":11}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'counters' should be an array in 'result-data-files-by-format'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"result-data-files-by-format\":{\"unit\":\"count\",\"counters\":[{\"value\":51}]}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from 'counters': Missing field 'counter-id'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"result-data-files-by-format\":{\"unit\":\"count\",\"counters\":[{\"counter-id\":\"AVRO\"}]}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from 'counters': Missing field 'value'");
  }

  @Test
  public void roundTripSerde() {
    ScanMetrics scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.DAYS);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics
        .resultDataFilesByFormat()
        .increment(new DefaultMetricTag(FileFormat.AVRO.toString()), 1L);
    scanMetrics
        .resultDataFilesByFormat()
        .increment(new DefaultMetricTag(FileFormat.ORC.toString()), 2L);
    scanMetrics
        .resultDataFilesByFormat()
        .increment(new DefaultMetricTag(FileFormat.PARQUET.toString()), 3L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.skippedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);
    scanMetrics.skippedDataFiles().increment(3L);
    scanMetrics.skippedDeleteFiles().increment(3L);
    scanMetrics.scannedDeleteManifests().increment(3L);
    scanMetrics.skippedDeleteManifests().increment(3L);
    scanMetrics.indexedDeleteFiles().increment(10L);
    scanMetrics.positionalDeleteFiles().increment(6L);
    scanMetrics.equalityDeleteFiles().increment(4L);

    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(scanMetrics);

    String expectedJson =
        "{\n"
            + "  \"total-planning-duration\" : {\n"
            + "    \"count\" : 1,\n"
            + "    \"time-unit\" : \"nanoseconds\",\n"
            + "    \"total-duration\" : 864000000000000\n"
            + "  },\n"
            + "  \"result-data-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"result-data-files-by-format\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"counters\" : [ {\n"
            + "      \"counter-id\" : \"AVRO\",\n"
            + "      \"value\" : 1\n"
            + "    }, {\n"
            + "      \"counter-id\" : \"ORC\",\n"
            + "      \"value\" : 2\n"
            + "    }, {\n"
            + "      \"counter-id\" : \"PARQUET\",\n"
            + "      \"value\" : 3\n"
            + "    } ]\n"
            + "  },\n"
            + "  \"result-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"total-data-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"total-delete-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 0\n"
            + "  },\n"
            + "  \"scanned-data-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"skipped-data-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"total-file-size-in-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 1069\n"
            + "  },\n"
            + "  \"total-delete-file-size-in-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 23\n"
            + "  },\n"
            + "  \"skipped-data-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 3\n"
            + "  },\n"
            + "  \"skipped-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 3\n"
            + "  },\n"
            + "  \"scanned-delete-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 3\n"
            + "  },\n"
            + "  \"skipped-delete-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 3\n"
            + "  },\n"
            + "  \"indexed-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 10\n"
            + "  },\n"
            + "  \"equality-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 4\n"
            + "  },\n"
            + "  \"positional-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 6\n"
            + "  }\n"
            + "}";

    String json = ScanMetricsResultParser.toJson(scanMetricsResult, true);
    Assertions.assertThat(ScanMetricsResultParser.fromJson(json)).isEqualTo(scanMetricsResult);

    Assertions.assertThatNoException()
        .isThrownBy(
            () -> JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.NON_EXTENSIBLE));
  }

  @Test
  public void roundTripSerdeNoopScanMetrics() {
    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(ScanMetrics.noop());
    String expectedJson = "{ }";

    String json = ScanMetricsResultParser.toJson(scanMetricsResult, true);
    Assertions.assertThat(json).isEqualTo(expectedJson);
    Assertions.assertThat(ScanMetricsResultParser.fromJson(json))
        .isEqualTo(ImmutableScanMetricsResult.builder().build());
  }
}
