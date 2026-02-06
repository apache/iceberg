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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.requests.ReportMetricsRequest.ReportType;
import org.apache.iceberg.util.JsonUtil;

public class ReportMetricsRequestParser {

  private static final String REPORT_TYPE = "report-type";

  private ReportMetricsRequestParser() {}

  public static String toJson(ReportMetricsRequest request) {
    return toJson(request, false);
  }

  public static String toJson(ReportMetricsRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(ReportMetricsRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid metrics request: null");

    gen.writeStartObject();

    gen.writeStringField(REPORT_TYPE, fromReportType(request.reportType()));

    if (ReportType.SCAN_REPORT == request.reportType()) {
      ScanReportParser.toJsonWithoutStartEnd((ScanReport) request.report(), gen);
    }

    if (ReportType.COMMIT_REPORT == request.reportType()) {
      CommitReportParser.toJsonWithoutStartEnd((CommitReport) request.report(), gen);
    }

    gen.writeEndObject();
  }

  private static String fromReportType(ReportType reportType) {
    return reportType.name().replaceAll("_", "-").toLowerCase(Locale.ENGLISH);
  }

  private static ReportType toReportType(String type) {
    return ReportType.fromString(type.replaceAll("-", "_"));
  }

  public static ReportMetricsRequest fromJson(String json) {
    return JsonUtil.parse(json, ReportMetricsRequestParser::fromJson);
  }

  public static ReportMetricsRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse metrics request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse metrics request from non-object: %s", json);

    ReportType type = toReportType(JsonUtil.getString(REPORT_TYPE, json));
    if (ReportType.SCAN_REPORT == type) {
      return ImmutableReportMetricsRequest.builder()
          .reportType(type)
          .report(ScanReportParser.fromJson(json))
          .build();
    }

    if (ReportType.COMMIT_REPORT == type) {
      return ImmutableReportMetricsRequest.builder()
          .reportType(type)
          .report(CommitReportParser.fromJson(json))
          .build();
    }

    return ReportMetricsRequest.unknown();
  }
}
