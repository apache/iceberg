package org.apache.iceberg.metrics;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ScanMetricsResult}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableScanMetricsResult.builder()}.
 */
@Generated(from = "ScanMetricsResult", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableScanMetricsResult implements ScanMetricsResult {
  private final @Nullable TimerResult totalPlanningDuration;
  private final @Nullable CounterResult resultDataFiles;
  private final @Nullable CounterResult resultDeleteFiles;
  private final @Nullable CounterResult totalDataManifests;
  private final @Nullable CounterResult totalDeleteManifests;
  private final @Nullable CounterResult scannedDataManifests;
  private final @Nullable CounterResult skippedDataManifests;
  private final @Nullable CounterResult totalFileSizeInBytes;
  private final @Nullable CounterResult totalDeleteFileSizeInBytes;
  private final @Nullable CounterResult skippedDataFiles;
  private final @Nullable CounterResult skippedDeleteFiles;
  private final @Nullable CounterResult scannedDeleteManifests;
  private final @Nullable CounterResult skippedDeleteManifests;
  private final @Nullable CounterResult indexedDeleteFiles;
  private final @Nullable CounterResult equalityDeleteFiles;
  private final @Nullable CounterResult positionalDeleteFiles;

  private ImmutableScanMetricsResult(
      @Nullable TimerResult totalPlanningDuration,
      @Nullable CounterResult resultDataFiles,
      @Nullable CounterResult resultDeleteFiles,
      @Nullable CounterResult totalDataManifests,
      @Nullable CounterResult totalDeleteManifests,
      @Nullable CounterResult scannedDataManifests,
      @Nullable CounterResult skippedDataManifests,
      @Nullable CounterResult totalFileSizeInBytes,
      @Nullable CounterResult totalDeleteFileSizeInBytes,
      @Nullable CounterResult skippedDataFiles,
      @Nullable CounterResult skippedDeleteFiles,
      @Nullable CounterResult scannedDeleteManifests,
      @Nullable CounterResult skippedDeleteManifests,
      @Nullable CounterResult indexedDeleteFiles,
      @Nullable CounterResult equalityDeleteFiles,
      @Nullable CounterResult positionalDeleteFiles) {
    this.totalPlanningDuration = totalPlanningDuration;
    this.resultDataFiles = resultDataFiles;
    this.resultDeleteFiles = resultDeleteFiles;
    this.totalDataManifests = totalDataManifests;
    this.totalDeleteManifests = totalDeleteManifests;
    this.scannedDataManifests = scannedDataManifests;
    this.skippedDataManifests = skippedDataManifests;
    this.totalFileSizeInBytes = totalFileSizeInBytes;
    this.totalDeleteFileSizeInBytes = totalDeleteFileSizeInBytes;
    this.skippedDataFiles = skippedDataFiles;
    this.skippedDeleteFiles = skippedDeleteFiles;
    this.scannedDeleteManifests = scannedDeleteManifests;
    this.skippedDeleteManifests = skippedDeleteManifests;
    this.indexedDeleteFiles = indexedDeleteFiles;
    this.equalityDeleteFiles = equalityDeleteFiles;
    this.positionalDeleteFiles = positionalDeleteFiles;
  }

  /**
   * @return The value of the {@code totalPlanningDuration} attribute
   */
  @Override
  public @Nullable TimerResult totalPlanningDuration() {
    return totalPlanningDuration;
  }

  /**
   * @return The value of the {@code resultDataFiles} attribute
   */
  @Override
  public @Nullable CounterResult resultDataFiles() {
    return resultDataFiles;
  }

  /**
   * @return The value of the {@code resultDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult resultDeleteFiles() {
    return resultDeleteFiles;
  }

  /**
   * @return The value of the {@code totalDataManifests} attribute
   */
  @Override
  public @Nullable CounterResult totalDataManifests() {
    return totalDataManifests;
  }

  /**
   * @return The value of the {@code totalDeleteManifests} attribute
   */
  @Override
  public @Nullable CounterResult totalDeleteManifests() {
    return totalDeleteManifests;
  }

  /**
   * @return The value of the {@code scannedDataManifests} attribute
   */
  @Override
  public @Nullable CounterResult scannedDataManifests() {
    return scannedDataManifests;
  }

  /**
   * @return The value of the {@code skippedDataManifests} attribute
   */
  @Override
  public @Nullable CounterResult skippedDataManifests() {
    return skippedDataManifests;
  }

  /**
   * @return The value of the {@code totalFileSizeInBytes} attribute
   */
  @Override
  public @Nullable CounterResult totalFileSizeInBytes() {
    return totalFileSizeInBytes;
  }

  /**
   * @return The value of the {@code totalDeleteFileSizeInBytes} attribute
   */
  @Override
  public @Nullable CounterResult totalDeleteFileSizeInBytes() {
    return totalDeleteFileSizeInBytes;
  }

  /**
   * @return The value of the {@code skippedDataFiles} attribute
   */
  @Override
  public @Nullable CounterResult skippedDataFiles() {
    return skippedDataFiles;
  }

  /**
   * @return The value of the {@code skippedDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult skippedDeleteFiles() {
    return skippedDeleteFiles;
  }

  /**
   * @return The value of the {@code scannedDeleteManifests} attribute
   */
  @Override
  public @Nullable CounterResult scannedDeleteManifests() {
    return scannedDeleteManifests;
  }

  /**
   * @return The value of the {@code skippedDeleteManifests} attribute
   */
  @Override
  public @Nullable CounterResult skippedDeleteManifests() {
    return skippedDeleteManifests;
  }

  /**
   * @return The value of the {@code indexedDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult indexedDeleteFiles() {
    return indexedDeleteFiles;
  }

  /**
   * @return The value of the {@code equalityDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult equalityDeleteFiles() {
    return equalityDeleteFiles;
  }

  /**
   * @return The value of the {@code positionalDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult positionalDeleteFiles() {
    return positionalDeleteFiles;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#totalPlanningDuration() totalPlanningDuration} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalPlanningDuration (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withTotalPlanningDuration(@Nullable TimerResult value) {
    if (this.totalPlanningDuration == value) return this;
    return new ImmutableScanMetricsResult(
        value,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#resultDataFiles() resultDataFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for resultDataFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withResultDataFiles(@Nullable CounterResult value) {
    if (this.resultDataFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        value,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#resultDeleteFiles() resultDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for resultDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withResultDeleteFiles(@Nullable CounterResult value) {
    if (this.resultDeleteFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        value,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#totalDataManifests() totalDataManifests} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDataManifests (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withTotalDataManifests(@Nullable CounterResult value) {
    if (this.totalDataManifests == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        value,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#totalDeleteManifests() totalDeleteManifests} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDeleteManifests (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withTotalDeleteManifests(@Nullable CounterResult value) {
    if (this.totalDeleteManifests == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        value,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#scannedDataManifests() scannedDataManifests} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for scannedDataManifests (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withScannedDataManifests(@Nullable CounterResult value) {
    if (this.scannedDataManifests == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        value,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#skippedDataManifests() skippedDataManifests} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for skippedDataManifests (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withSkippedDataManifests(@Nullable CounterResult value) {
    if (this.skippedDataManifests == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        value,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#totalFileSizeInBytes() totalFileSizeInBytes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalFileSizeInBytes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withTotalFileSizeInBytes(@Nullable CounterResult value) {
    if (this.totalFileSizeInBytes == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        value,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#totalDeleteFileSizeInBytes() totalDeleteFileSizeInBytes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDeleteFileSizeInBytes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withTotalDeleteFileSizeInBytes(@Nullable CounterResult value) {
    if (this.totalDeleteFileSizeInBytes == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        value,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#skippedDataFiles() skippedDataFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for skippedDataFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withSkippedDataFiles(@Nullable CounterResult value) {
    if (this.skippedDataFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        value,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#skippedDeleteFiles() skippedDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for skippedDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withSkippedDeleteFiles(@Nullable CounterResult value) {
    if (this.skippedDeleteFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        value,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#scannedDeleteManifests() scannedDeleteManifests} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for scannedDeleteManifests (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withScannedDeleteManifests(@Nullable CounterResult value) {
    if (this.scannedDeleteManifests == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        value,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#skippedDeleteManifests() skippedDeleteManifests} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for skippedDeleteManifests (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withSkippedDeleteManifests(@Nullable CounterResult value) {
    if (this.skippedDeleteManifests == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        value,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#indexedDeleteFiles() indexedDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for indexedDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withIndexedDeleteFiles(@Nullable CounterResult value) {
    if (this.indexedDeleteFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        value,
        this.equalityDeleteFiles,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#equalityDeleteFiles() equalityDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for equalityDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withEqualityDeleteFiles(@Nullable CounterResult value) {
    if (this.equalityDeleteFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        value,
        this.positionalDeleteFiles);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetricsResult#positionalDeleteFiles() positionalDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for positionalDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetricsResult withPositionalDeleteFiles(@Nullable CounterResult value) {
    if (this.positionalDeleteFiles == value) return this;
    return new ImmutableScanMetricsResult(
        this.totalPlanningDuration,
        this.resultDataFiles,
        this.resultDeleteFiles,
        this.totalDataManifests,
        this.totalDeleteManifests,
        this.scannedDataManifests,
        this.skippedDataManifests,
        this.totalFileSizeInBytes,
        this.totalDeleteFileSizeInBytes,
        this.skippedDataFiles,
        this.skippedDeleteFiles,
        this.scannedDeleteManifests,
        this.skippedDeleteManifests,
        this.indexedDeleteFiles,
        this.equalityDeleteFiles,
        value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableScanMetricsResult} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableScanMetricsResult
        && equalTo(0, (ImmutableScanMetricsResult) another);
  }

  private boolean equalTo(int synthetic, ImmutableScanMetricsResult another) {
    return Objects.equals(totalPlanningDuration, another.totalPlanningDuration)
        && Objects.equals(resultDataFiles, another.resultDataFiles)
        && Objects.equals(resultDeleteFiles, another.resultDeleteFiles)
        && Objects.equals(totalDataManifests, another.totalDataManifests)
        && Objects.equals(totalDeleteManifests, another.totalDeleteManifests)
        && Objects.equals(scannedDataManifests, another.scannedDataManifests)
        && Objects.equals(skippedDataManifests, another.skippedDataManifests)
        && Objects.equals(totalFileSizeInBytes, another.totalFileSizeInBytes)
        && Objects.equals(totalDeleteFileSizeInBytes, another.totalDeleteFileSizeInBytes)
        && Objects.equals(skippedDataFiles, another.skippedDataFiles)
        && Objects.equals(skippedDeleteFiles, another.skippedDeleteFiles)
        && Objects.equals(scannedDeleteManifests, another.scannedDeleteManifests)
        && Objects.equals(skippedDeleteManifests, another.skippedDeleteManifests)
        && Objects.equals(indexedDeleteFiles, another.indexedDeleteFiles)
        && Objects.equals(equalityDeleteFiles, another.equalityDeleteFiles)
        && Objects.equals(positionalDeleteFiles, another.positionalDeleteFiles);
  }

  /**
   * Computes a hash code from attributes: {@code totalPlanningDuration}, {@code resultDataFiles}, {@code resultDeleteFiles}, {@code totalDataManifests}, {@code totalDeleteManifests}, {@code scannedDataManifests}, {@code skippedDataManifests}, {@code totalFileSizeInBytes}, {@code totalDeleteFileSizeInBytes}, {@code skippedDataFiles}, {@code skippedDeleteFiles}, {@code scannedDeleteManifests}, {@code skippedDeleteManifests}, {@code indexedDeleteFiles}, {@code equalityDeleteFiles}, {@code positionalDeleteFiles}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(totalPlanningDuration);
    h += (h << 5) + Objects.hashCode(resultDataFiles);
    h += (h << 5) + Objects.hashCode(resultDeleteFiles);
    h += (h << 5) + Objects.hashCode(totalDataManifests);
    h += (h << 5) + Objects.hashCode(totalDeleteManifests);
    h += (h << 5) + Objects.hashCode(scannedDataManifests);
    h += (h << 5) + Objects.hashCode(skippedDataManifests);
    h += (h << 5) + Objects.hashCode(totalFileSizeInBytes);
    h += (h << 5) + Objects.hashCode(totalDeleteFileSizeInBytes);
    h += (h << 5) + Objects.hashCode(skippedDataFiles);
    h += (h << 5) + Objects.hashCode(skippedDeleteFiles);
    h += (h << 5) + Objects.hashCode(scannedDeleteManifests);
    h += (h << 5) + Objects.hashCode(skippedDeleteManifests);
    h += (h << 5) + Objects.hashCode(indexedDeleteFiles);
    h += (h << 5) + Objects.hashCode(equalityDeleteFiles);
    h += (h << 5) + Objects.hashCode(positionalDeleteFiles);
    return h;
  }

  /**
   * Prints the immutable value {@code ScanMetricsResult} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ScanMetricsResult{"
        + "totalPlanningDuration=" + totalPlanningDuration
        + ", resultDataFiles=" + resultDataFiles
        + ", resultDeleteFiles=" + resultDeleteFiles
        + ", totalDataManifests=" + totalDataManifests
        + ", totalDeleteManifests=" + totalDeleteManifests
        + ", scannedDataManifests=" + scannedDataManifests
        + ", skippedDataManifests=" + skippedDataManifests
        + ", totalFileSizeInBytes=" + totalFileSizeInBytes
        + ", totalDeleteFileSizeInBytes=" + totalDeleteFileSizeInBytes
        + ", skippedDataFiles=" + skippedDataFiles
        + ", skippedDeleteFiles=" + skippedDeleteFiles
        + ", scannedDeleteManifests=" + scannedDeleteManifests
        + ", skippedDeleteManifests=" + skippedDeleteManifests
        + ", indexedDeleteFiles=" + indexedDeleteFiles
        + ", equalityDeleteFiles=" + equalityDeleteFiles
        + ", positionalDeleteFiles=" + positionalDeleteFiles
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ScanMetricsResult} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ScanMetricsResult instance
   */
  public static ImmutableScanMetricsResult copyOf(ScanMetricsResult instance) {
    if (instance instanceof ImmutableScanMetricsResult) {
      return (ImmutableScanMetricsResult) instance;
    }
    return ImmutableScanMetricsResult.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableScanMetricsResult ImmutableScanMetricsResult}.
   * <pre>
   * ImmutableScanMetricsResult.builder()
   *    .totalPlanningDuration(org.apache.iceberg.metrics.TimerResult | null) // nullable {@link ScanMetricsResult#totalPlanningDuration() totalPlanningDuration}
   *    .resultDataFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#resultDataFiles() resultDataFiles}
   *    .resultDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#resultDeleteFiles() resultDeleteFiles}
   *    .totalDataManifests(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#totalDataManifests() totalDataManifests}
   *    .totalDeleteManifests(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#totalDeleteManifests() totalDeleteManifests}
   *    .scannedDataManifests(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#scannedDataManifests() scannedDataManifests}
   *    .skippedDataManifests(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#skippedDataManifests() skippedDataManifests}
   *    .totalFileSizeInBytes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#totalFileSizeInBytes() totalFileSizeInBytes}
   *    .totalDeleteFileSizeInBytes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#totalDeleteFileSizeInBytes() totalDeleteFileSizeInBytes}
   *    .skippedDataFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#skippedDataFiles() skippedDataFiles}
   *    .skippedDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#skippedDeleteFiles() skippedDeleteFiles}
   *    .scannedDeleteManifests(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#scannedDeleteManifests() scannedDeleteManifests}
   *    .skippedDeleteManifests(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#skippedDeleteManifests() skippedDeleteManifests}
   *    .indexedDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#indexedDeleteFiles() indexedDeleteFiles}
   *    .equalityDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#equalityDeleteFiles() equalityDeleteFiles}
   *    .positionalDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link ScanMetricsResult#positionalDeleteFiles() positionalDeleteFiles}
   *    .build();
   * </pre>
   * @return A new ImmutableScanMetricsResult builder
   */
  public static ImmutableScanMetricsResult.Builder builder() {
    return new ImmutableScanMetricsResult.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableScanMetricsResult ImmutableScanMetricsResult}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ScanMetricsResult", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private @Nullable TimerResult totalPlanningDuration;
    private @Nullable CounterResult resultDataFiles;
    private @Nullable CounterResult resultDeleteFiles;
    private @Nullable CounterResult totalDataManifests;
    private @Nullable CounterResult totalDeleteManifests;
    private @Nullable CounterResult scannedDataManifests;
    private @Nullable CounterResult skippedDataManifests;
    private @Nullable CounterResult totalFileSizeInBytes;
    private @Nullable CounterResult totalDeleteFileSizeInBytes;
    private @Nullable CounterResult skippedDataFiles;
    private @Nullable CounterResult skippedDeleteFiles;
    private @Nullable CounterResult scannedDeleteManifests;
    private @Nullable CounterResult skippedDeleteManifests;
    private @Nullable CounterResult indexedDeleteFiles;
    private @Nullable CounterResult equalityDeleteFiles;
    private @Nullable CounterResult positionalDeleteFiles;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ScanMetricsResult} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ScanMetricsResult instance) {
      Objects.requireNonNull(instance, "instance");
      @Nullable TimerResult totalPlanningDurationValue = instance.totalPlanningDuration();
      if (totalPlanningDurationValue != null) {
        totalPlanningDuration(totalPlanningDurationValue);
      }
      @Nullable CounterResult resultDataFilesValue = instance.resultDataFiles();
      if (resultDataFilesValue != null) {
        resultDataFiles(resultDataFilesValue);
      }
      @Nullable CounterResult resultDeleteFilesValue = instance.resultDeleteFiles();
      if (resultDeleteFilesValue != null) {
        resultDeleteFiles(resultDeleteFilesValue);
      }
      @Nullable CounterResult totalDataManifestsValue = instance.totalDataManifests();
      if (totalDataManifestsValue != null) {
        totalDataManifests(totalDataManifestsValue);
      }
      @Nullable CounterResult totalDeleteManifestsValue = instance.totalDeleteManifests();
      if (totalDeleteManifestsValue != null) {
        totalDeleteManifests(totalDeleteManifestsValue);
      }
      @Nullable CounterResult scannedDataManifestsValue = instance.scannedDataManifests();
      if (scannedDataManifestsValue != null) {
        scannedDataManifests(scannedDataManifestsValue);
      }
      @Nullable CounterResult skippedDataManifestsValue = instance.skippedDataManifests();
      if (skippedDataManifestsValue != null) {
        skippedDataManifests(skippedDataManifestsValue);
      }
      @Nullable CounterResult totalFileSizeInBytesValue = instance.totalFileSizeInBytes();
      if (totalFileSizeInBytesValue != null) {
        totalFileSizeInBytes(totalFileSizeInBytesValue);
      }
      @Nullable CounterResult totalDeleteFileSizeInBytesValue = instance.totalDeleteFileSizeInBytes();
      if (totalDeleteFileSizeInBytesValue != null) {
        totalDeleteFileSizeInBytes(totalDeleteFileSizeInBytesValue);
      }
      @Nullable CounterResult skippedDataFilesValue = instance.skippedDataFiles();
      if (skippedDataFilesValue != null) {
        skippedDataFiles(skippedDataFilesValue);
      }
      @Nullable CounterResult skippedDeleteFilesValue = instance.skippedDeleteFiles();
      if (skippedDeleteFilesValue != null) {
        skippedDeleteFiles(skippedDeleteFilesValue);
      }
      @Nullable CounterResult scannedDeleteManifestsValue = instance.scannedDeleteManifests();
      if (scannedDeleteManifestsValue != null) {
        scannedDeleteManifests(scannedDeleteManifestsValue);
      }
      @Nullable CounterResult skippedDeleteManifestsValue = instance.skippedDeleteManifests();
      if (skippedDeleteManifestsValue != null) {
        skippedDeleteManifests(skippedDeleteManifestsValue);
      }
      @Nullable CounterResult indexedDeleteFilesValue = instance.indexedDeleteFiles();
      if (indexedDeleteFilesValue != null) {
        indexedDeleteFiles(indexedDeleteFilesValue);
      }
      @Nullable CounterResult equalityDeleteFilesValue = instance.equalityDeleteFiles();
      if (equalityDeleteFilesValue != null) {
        equalityDeleteFiles(equalityDeleteFilesValue);
      }
      @Nullable CounterResult positionalDeleteFilesValue = instance.positionalDeleteFiles();
      if (positionalDeleteFilesValue != null) {
        positionalDeleteFiles(positionalDeleteFilesValue);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#totalPlanningDuration() totalPlanningDuration} attribute.
     * @param totalPlanningDuration The value for totalPlanningDuration (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalPlanningDuration(@Nullable TimerResult totalPlanningDuration) {
      this.totalPlanningDuration = totalPlanningDuration;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#resultDataFiles() resultDataFiles} attribute.
     * @param resultDataFiles The value for resultDataFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder resultDataFiles(@Nullable CounterResult resultDataFiles) {
      this.resultDataFiles = resultDataFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#resultDeleteFiles() resultDeleteFiles} attribute.
     * @param resultDeleteFiles The value for resultDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder resultDeleteFiles(@Nullable CounterResult resultDeleteFiles) {
      this.resultDeleteFiles = resultDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#totalDataManifests() totalDataManifests} attribute.
     * @param totalDataManifests The value for totalDataManifests (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDataManifests(@Nullable CounterResult totalDataManifests) {
      this.totalDataManifests = totalDataManifests;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#totalDeleteManifests() totalDeleteManifests} attribute.
     * @param totalDeleteManifests The value for totalDeleteManifests (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDeleteManifests(@Nullable CounterResult totalDeleteManifests) {
      this.totalDeleteManifests = totalDeleteManifests;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#scannedDataManifests() scannedDataManifests} attribute.
     * @param scannedDataManifests The value for scannedDataManifests (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scannedDataManifests(@Nullable CounterResult scannedDataManifests) {
      this.scannedDataManifests = scannedDataManifests;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#skippedDataManifests() skippedDataManifests} attribute.
     * @param skippedDataManifests The value for skippedDataManifests (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder skippedDataManifests(@Nullable CounterResult skippedDataManifests) {
      this.skippedDataManifests = skippedDataManifests;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#totalFileSizeInBytes() totalFileSizeInBytes} attribute.
     * @param totalFileSizeInBytes The value for totalFileSizeInBytes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalFileSizeInBytes(@Nullable CounterResult totalFileSizeInBytes) {
      this.totalFileSizeInBytes = totalFileSizeInBytes;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#totalDeleteFileSizeInBytes() totalDeleteFileSizeInBytes} attribute.
     * @param totalDeleteFileSizeInBytes The value for totalDeleteFileSizeInBytes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDeleteFileSizeInBytes(@Nullable CounterResult totalDeleteFileSizeInBytes) {
      this.totalDeleteFileSizeInBytes = totalDeleteFileSizeInBytes;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#skippedDataFiles() skippedDataFiles} attribute.
     * @param skippedDataFiles The value for skippedDataFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder skippedDataFiles(@Nullable CounterResult skippedDataFiles) {
      this.skippedDataFiles = skippedDataFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#skippedDeleteFiles() skippedDeleteFiles} attribute.
     * @param skippedDeleteFiles The value for skippedDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder skippedDeleteFiles(@Nullable CounterResult skippedDeleteFiles) {
      this.skippedDeleteFiles = skippedDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#scannedDeleteManifests() scannedDeleteManifests} attribute.
     * @param scannedDeleteManifests The value for scannedDeleteManifests (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scannedDeleteManifests(@Nullable CounterResult scannedDeleteManifests) {
      this.scannedDeleteManifests = scannedDeleteManifests;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#skippedDeleteManifests() skippedDeleteManifests} attribute.
     * @param skippedDeleteManifests The value for skippedDeleteManifests (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder skippedDeleteManifests(@Nullable CounterResult skippedDeleteManifests) {
      this.skippedDeleteManifests = skippedDeleteManifests;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#indexedDeleteFiles() indexedDeleteFiles} attribute.
     * @param indexedDeleteFiles The value for indexedDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder indexedDeleteFiles(@Nullable CounterResult indexedDeleteFiles) {
      this.indexedDeleteFiles = indexedDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#equalityDeleteFiles() equalityDeleteFiles} attribute.
     * @param equalityDeleteFiles The value for equalityDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder equalityDeleteFiles(@Nullable CounterResult equalityDeleteFiles) {
      this.equalityDeleteFiles = equalityDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetricsResult#positionalDeleteFiles() positionalDeleteFiles} attribute.
     * @param positionalDeleteFiles The value for positionalDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder positionalDeleteFiles(@Nullable CounterResult positionalDeleteFiles) {
      this.positionalDeleteFiles = positionalDeleteFiles;
      return this;
    }

    /**
     * Builds a new {@link ImmutableScanMetricsResult ImmutableScanMetricsResult}.
     * @return An immutable instance of ScanMetricsResult
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableScanMetricsResult build() {
      return new ImmutableScanMetricsResult(
          totalPlanningDuration,
          resultDataFiles,
          resultDeleteFiles,
          totalDataManifests,
          totalDeleteManifests,
          scannedDataManifests,
          skippedDataManifests,
          totalFileSizeInBytes,
          totalDeleteFileSizeInBytes,
          skippedDataFiles,
          skippedDeleteFiles,
          scannedDeleteManifests,
          skippedDeleteManifests,
          indexedDeleteFiles,
          equalityDeleteFiles,
          positionalDeleteFiles);
    }
  }
}
