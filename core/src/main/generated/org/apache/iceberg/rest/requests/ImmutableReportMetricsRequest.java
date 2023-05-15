package org.apache.iceberg.rest.requests;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.iceberg.metrics.MetricsReport;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ReportMetricsRequest}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableReportMetricsRequest.builder()}.
 */
@Generated(from = "ReportMetricsRequest", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableReportMetricsRequest
    implements ReportMetricsRequest {
  private final ReportMetricsRequest.ReportType reportType;
  private final MetricsReport report;

  private ImmutableReportMetricsRequest(
      ReportMetricsRequest.ReportType reportType,
      MetricsReport report) {
    this.reportType = reportType;
    this.report = report;
  }

  /**
   * @return The value of the {@code reportType} attribute
   */
  @Override
  public ReportMetricsRequest.ReportType reportType() {
    return reportType;
  }

  /**
   * @return The value of the {@code report} attribute
   */
  @Override
  public MetricsReport report() {
    return report;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ReportMetricsRequest#reportType() reportType} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for reportType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableReportMetricsRequest withReportType(ReportMetricsRequest.ReportType value) {
    ReportMetricsRequest.ReportType newValue = Objects.requireNonNull(value, "reportType");
    if (this.reportType == newValue) return this;
    return new ImmutableReportMetricsRequest(newValue, this.report);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ReportMetricsRequest#report() report} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for report
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableReportMetricsRequest withReport(MetricsReport value) {
    if (this.report == value) return this;
    MetricsReport newValue = Objects.requireNonNull(value, "report");
    return new ImmutableReportMetricsRequest(this.reportType, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableReportMetricsRequest} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableReportMetricsRequest
        && equalTo(0, (ImmutableReportMetricsRequest) another);
  }

  private boolean equalTo(int synthetic, ImmutableReportMetricsRequest another) {
    return reportType.equals(another.reportType)
        && report.equals(another.report);
  }

  /**
   * Computes a hash code from attributes: {@code reportType}, {@code report}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + reportType.hashCode();
    h += (h << 5) + report.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ReportMetricsRequest} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ReportMetricsRequest{"
        + "reportType=" + reportType
        + ", report=" + report
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ReportMetricsRequest} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ReportMetricsRequest instance
   */
  public static ImmutableReportMetricsRequest copyOf(ReportMetricsRequest instance) {
    if (instance instanceof ImmutableReportMetricsRequest) {
      return (ImmutableReportMetricsRequest) instance;
    }
    return ImmutableReportMetricsRequest.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableReportMetricsRequest ImmutableReportMetricsRequest}.
   * <pre>
   * ImmutableReportMetricsRequest.builder()
   *    .reportType(org.apache.iceberg.rest.requests.ReportMetricsRequest.ReportType) // required {@link ReportMetricsRequest#reportType() reportType}
   *    .report(org.apache.iceberg.metrics.MetricsReport) // required {@link ReportMetricsRequest#report() report}
   *    .build();
   * </pre>
   * @return A new ImmutableReportMetricsRequest builder
   */
  public static ImmutableReportMetricsRequest.Builder builder() {
    return new ImmutableReportMetricsRequest.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableReportMetricsRequest ImmutableReportMetricsRequest}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ReportMetricsRequest", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_REPORT_TYPE = 0x1L;
    private static final long INIT_BIT_REPORT = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ReportMetricsRequest.ReportType reportType;
    private @Nullable MetricsReport report;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ReportMetricsRequest} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ReportMetricsRequest instance) {
      Objects.requireNonNull(instance, "instance");
      reportType(instance.reportType());
      report(instance.report());
      return this;
    }

    /**
     * Initializes the value for the {@link ReportMetricsRequest#reportType() reportType} attribute.
     * @param reportType The value for reportType 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder reportType(ReportMetricsRequest.ReportType reportType) {
      this.reportType = Objects.requireNonNull(reportType, "reportType");
      initBits &= ~INIT_BIT_REPORT_TYPE;
      return this;
    }

    /**
     * Initializes the value for the {@link ReportMetricsRequest#report() report} attribute.
     * @param report The value for report 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder report(MetricsReport report) {
      this.report = Objects.requireNonNull(report, "report");
      initBits &= ~INIT_BIT_REPORT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableReportMetricsRequest ImmutableReportMetricsRequest}.
     * @return An immutable instance of ReportMetricsRequest
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableReportMetricsRequest build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableReportMetricsRequest(reportType, report);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_REPORT_TYPE) != 0) attributes.add("reportType");
      if ((initBits & INIT_BIT_REPORT) != 0) attributes.add("report");
      return "Cannot build ReportMetricsRequest, some of required attributes are not set " + attributes;
    }
  }
}
