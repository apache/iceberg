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

import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class ReportMetricsRequest implements RESTRequest {

  public enum ReportType {
    UNKNOWN,
    SCAN_REPORT
  }

  private MetricsReport report;
  private ReportType reportType;

  @SuppressWarnings("unused")
  public ReportMetricsRequest() {
    // Needed for Jackson Deserialization.
  }

  private ReportMetricsRequest(MetricsReport report) {
    this.report = report;
    reportType = report instanceof ScanReport ? ReportType.SCAN_REPORT : ReportType.UNKNOWN;
    validate();
  }

  public MetricsReport report() {
    return report;
  }

  public ReportType reportType() {
    return reportType;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("report", report).toString();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
  }

  public static ReportMetricsRequest.Builder builder() {
    return new ReportMetricsRequest.Builder();
  }

  public static class Builder {
    private MetricsReport report;

    private Builder() {}

    public Builder fromReport(MetricsReport newReport) {
      this.report = newReport;
      return this;
    }

    public ReportMetricsRequest build() {
      Preconditions.checkArgument(null != report, "Invalid metrics report: null");
      return new ReportMetricsRequest(report);
    }
  }
}
