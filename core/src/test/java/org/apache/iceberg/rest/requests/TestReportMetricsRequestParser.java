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
package org.apache.iceberg.rest.requests;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReport.ScanMetrics;
import org.apache.iceberg.metrics.ScanReport.ScanMetricsResult;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestReportMetricsRequestParser {

  @Test
  public void nullCheck() {
    Assertions.assertThatThrownBy(() -> ReportMetricsRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metrics request: null");

    Assertions.assertThatThrownBy(() -> ReportMetricsRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metrics request from null object");
  }

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> ReportMetricsRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: report-type");

    Assertions.assertThatThrownBy(
            () -> ReportMetricsRequestParser.fromJson("{\"report-type\":\"scan-report\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: table-name");

    Assertions.assertThatThrownBy(
            () ->
                ReportMetricsRequestParser.fromJson(
                    "{\"report-type\":\"scan-report\", \"table-name\" : \"x\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: snapshot-id");
  }

  @Test
  public void invalidReportType() {
    Assertions.assertThatThrownBy(
            () -> ReportMetricsRequestParser.fromJson("{\"report-type\":\"invalid\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid report type: invalid");

    Assertions.assertThatThrownBy(
            () ->
                ReportMetricsRequestParser.fromJson(
                    ReportMetricsRequestParser.toJson(
                        ReportMetricsRequest.of(new MetricsReport() {}))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Unsupported report type: org.apache.iceberg.rest.requests.TestReportMetricsRequestParser$1");
  }

  @Test
  public void roundTripSerde() {
    String tableName = "roundTripTableName";
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .projection(projection)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    String expectedJson =
        "{\n"
            + "  \"report-type\" : \"scan-report\",\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"filter\" : true,\n"
            + "  \"projection\" : {\n"
            + "    \"type\" : \"struct\",\n"
            + "    \"schema-id\" : 0,\n"
            + "    \"fields\" : [ {\n"
            + "      \"id\" : 1,\n"
            + "      \"name\" : \"c1\",\n"
            + "      \"required\" : true,\n"
            + "      \"type\" : \"string\",\n"
            + "      \"doc\" : \"c1\"\n"
            + "    } ]\n"
            + "  },\n"
            + "  \"metrics\" : { }\n"
            + "}";

    ReportMetricsRequest metricsRequest = ReportMetricsRequest.of(scanReport);

    String json = ReportMetricsRequestParser.toJson(metricsRequest, true);
    Assertions.assertThat(json).isEqualTo(expectedJson);

    Assertions.assertThat(ReportMetricsRequestParser.fromJson(json).report())
        .usingRecursiveComparison()
        .ignoringFields("projection")
        .isEqualTo(metricsRequest.report());
  }
}
