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

import java.util.Locale;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;
import org.immutables.value.Value;

@Value.Immutable
public interface ReportMetricsRequest extends RESTRequest {

  enum ReportType {
    SCAN_REPORT;

    static ReportType fromString(String reportType) {
      Preconditions.checkArgument(null != reportType, "Invalid report type: null");
      try {
        return ReportType.valueOf(reportType.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("Invalid report type: %s", reportType), e);
      }
    }
  }

  ReportType reportType();

  MetricsReport report();

  @Override
  default void validate() {
    // nothing to do here as it's not possible to create a ReportMetricsRequest where
    // report/reportType is null
  }

  static ReportMetricsRequest of(MetricsReport report) {
    ReportType reportType = report instanceof ScanReport ? ReportType.SCAN_REPORT : null;
    Preconditions.checkArgument(
        null != reportType, "Unsupported report type: %s", report.getClass().getName());

    return ImmutableReportMetricsRequest.builder().reportType(reportType).report(report).build();
  }
}
