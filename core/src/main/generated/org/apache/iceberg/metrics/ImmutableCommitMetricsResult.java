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
 * Immutable implementation of {@link CommitMetricsResult}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCommitMetricsResult.builder()}.
 */
@Generated(from = "CommitMetricsResult", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableCommitMetricsResult implements CommitMetricsResult {
  private final @Nullable TimerResult totalDuration;
  private final @Nullable CounterResult attempts;
  private final @Nullable CounterResult addedDataFiles;
  private final @Nullable CounterResult removedDataFiles;
  private final @Nullable CounterResult totalDataFiles;
  private final @Nullable CounterResult addedDeleteFiles;
  private final @Nullable CounterResult addedEqualityDeleteFiles;
  private final @Nullable CounterResult addedPositionalDeleteFiles;
  private final @Nullable CounterResult removedDeleteFiles;
  private final @Nullable CounterResult removedEqualityDeleteFiles;
  private final @Nullable CounterResult removedPositionalDeleteFiles;
  private final @Nullable CounterResult totalDeleteFiles;
  private final @Nullable CounterResult addedRecords;
  private final @Nullable CounterResult removedRecords;
  private final @Nullable CounterResult totalRecords;
  private final @Nullable CounterResult addedFilesSizeInBytes;
  private final @Nullable CounterResult removedFilesSizeInBytes;
  private final @Nullable CounterResult totalFilesSizeInBytes;
  private final @Nullable CounterResult addedPositionalDeletes;
  private final @Nullable CounterResult removedPositionalDeletes;
  private final @Nullable CounterResult totalPositionalDeletes;
  private final @Nullable CounterResult addedEqualityDeletes;
  private final @Nullable CounterResult removedEqualityDeletes;
  private final @Nullable CounterResult totalEqualityDeletes;

  private ImmutableCommitMetricsResult(
      @Nullable TimerResult totalDuration,
      @Nullable CounterResult attempts,
      @Nullable CounterResult addedDataFiles,
      @Nullable CounterResult removedDataFiles,
      @Nullable CounterResult totalDataFiles,
      @Nullable CounterResult addedDeleteFiles,
      @Nullable CounterResult addedEqualityDeleteFiles,
      @Nullable CounterResult addedPositionalDeleteFiles,
      @Nullable CounterResult removedDeleteFiles,
      @Nullable CounterResult removedEqualityDeleteFiles,
      @Nullable CounterResult removedPositionalDeleteFiles,
      @Nullable CounterResult totalDeleteFiles,
      @Nullable CounterResult addedRecords,
      @Nullable CounterResult removedRecords,
      @Nullable CounterResult totalRecords,
      @Nullable CounterResult addedFilesSizeInBytes,
      @Nullable CounterResult removedFilesSizeInBytes,
      @Nullable CounterResult totalFilesSizeInBytes,
      @Nullable CounterResult addedPositionalDeletes,
      @Nullable CounterResult removedPositionalDeletes,
      @Nullable CounterResult totalPositionalDeletes,
      @Nullable CounterResult addedEqualityDeletes,
      @Nullable CounterResult removedEqualityDeletes,
      @Nullable CounterResult totalEqualityDeletes) {
    this.totalDuration = totalDuration;
    this.attempts = attempts;
    this.addedDataFiles = addedDataFiles;
    this.removedDataFiles = removedDataFiles;
    this.totalDataFiles = totalDataFiles;
    this.addedDeleteFiles = addedDeleteFiles;
    this.addedEqualityDeleteFiles = addedEqualityDeleteFiles;
    this.addedPositionalDeleteFiles = addedPositionalDeleteFiles;
    this.removedDeleteFiles = removedDeleteFiles;
    this.removedEqualityDeleteFiles = removedEqualityDeleteFiles;
    this.removedPositionalDeleteFiles = removedPositionalDeleteFiles;
    this.totalDeleteFiles = totalDeleteFiles;
    this.addedRecords = addedRecords;
    this.removedRecords = removedRecords;
    this.totalRecords = totalRecords;
    this.addedFilesSizeInBytes = addedFilesSizeInBytes;
    this.removedFilesSizeInBytes = removedFilesSizeInBytes;
    this.totalFilesSizeInBytes = totalFilesSizeInBytes;
    this.addedPositionalDeletes = addedPositionalDeletes;
    this.removedPositionalDeletes = removedPositionalDeletes;
    this.totalPositionalDeletes = totalPositionalDeletes;
    this.addedEqualityDeletes = addedEqualityDeletes;
    this.removedEqualityDeletes = removedEqualityDeletes;
    this.totalEqualityDeletes = totalEqualityDeletes;
  }

  /**
   * @return The value of the {@code totalDuration} attribute
   */
  @Override
  public @Nullable TimerResult totalDuration() {
    return totalDuration;
  }

  /**
   * @return The value of the {@code attempts} attribute
   */
  @Override
  public @Nullable CounterResult attempts() {
    return attempts;
  }

  /**
   * @return The value of the {@code addedDataFiles} attribute
   */
  @Override
  public @Nullable CounterResult addedDataFiles() {
    return addedDataFiles;
  }

  /**
   * @return The value of the {@code removedDataFiles} attribute
   */
  @Override
  public @Nullable CounterResult removedDataFiles() {
    return removedDataFiles;
  }

  /**
   * @return The value of the {@code totalDataFiles} attribute
   */
  @Override
  public @Nullable CounterResult totalDataFiles() {
    return totalDataFiles;
  }

  /**
   * @return The value of the {@code addedDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult addedDeleteFiles() {
    return addedDeleteFiles;
  }

  /**
   * @return The value of the {@code addedEqualityDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult addedEqualityDeleteFiles() {
    return addedEqualityDeleteFiles;
  }

  /**
   * @return The value of the {@code addedPositionalDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult addedPositionalDeleteFiles() {
    return addedPositionalDeleteFiles;
  }

  /**
   * @return The value of the {@code removedDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult removedDeleteFiles() {
    return removedDeleteFiles;
  }

  /**
   * @return The value of the {@code removedEqualityDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult removedEqualityDeleteFiles() {
    return removedEqualityDeleteFiles;
  }

  /**
   * @return The value of the {@code removedPositionalDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult removedPositionalDeleteFiles() {
    return removedPositionalDeleteFiles;
  }

  /**
   * @return The value of the {@code totalDeleteFiles} attribute
   */
  @Override
  public @Nullable CounterResult totalDeleteFiles() {
    return totalDeleteFiles;
  }

  /**
   * @return The value of the {@code addedRecords} attribute
   */
  @Override
  public @Nullable CounterResult addedRecords() {
    return addedRecords;
  }

  /**
   * @return The value of the {@code removedRecords} attribute
   */
  @Override
  public @Nullable CounterResult removedRecords() {
    return removedRecords;
  }

  /**
   * @return The value of the {@code totalRecords} attribute
   */
  @Override
  public @Nullable CounterResult totalRecords() {
    return totalRecords;
  }

  /**
   * @return The value of the {@code addedFilesSizeInBytes} attribute
   */
  @Override
  public @Nullable CounterResult addedFilesSizeInBytes() {
    return addedFilesSizeInBytes;
  }

  /**
   * @return The value of the {@code removedFilesSizeInBytes} attribute
   */
  @Override
  public @Nullable CounterResult removedFilesSizeInBytes() {
    return removedFilesSizeInBytes;
  }

  /**
   * @return The value of the {@code totalFilesSizeInBytes} attribute
   */
  @Override
  public @Nullable CounterResult totalFilesSizeInBytes() {
    return totalFilesSizeInBytes;
  }

  /**
   * @return The value of the {@code addedPositionalDeletes} attribute
   */
  @Override
  public @Nullable CounterResult addedPositionalDeletes() {
    return addedPositionalDeletes;
  }

  /**
   * @return The value of the {@code removedPositionalDeletes} attribute
   */
  @Override
  public @Nullable CounterResult removedPositionalDeletes() {
    return removedPositionalDeletes;
  }

  /**
   * @return The value of the {@code totalPositionalDeletes} attribute
   */
  @Override
  public @Nullable CounterResult totalPositionalDeletes() {
    return totalPositionalDeletes;
  }

  /**
   * @return The value of the {@code addedEqualityDeletes} attribute
   */
  @Override
  public @Nullable CounterResult addedEqualityDeletes() {
    return addedEqualityDeletes;
  }

  /**
   * @return The value of the {@code removedEqualityDeletes} attribute
   */
  @Override
  public @Nullable CounterResult removedEqualityDeletes() {
    return removedEqualityDeletes;
  }

  /**
   * @return The value of the {@code totalEqualityDeletes} attribute
   */
  @Override
  public @Nullable CounterResult totalEqualityDeletes() {
    return totalEqualityDeletes;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalDuration() totalDuration} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDuration (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalDuration(@Nullable TimerResult value) {
    if (this.totalDuration == value) return this;
    return new ImmutableCommitMetricsResult(
        value,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#attempts() attempts} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for attempts (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAttempts(@Nullable CounterResult value) {
    if (this.attempts == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        value,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedDataFiles() addedDataFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedDataFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedDataFiles(@Nullable CounterResult value) {
    if (this.addedDataFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        value,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedDataFiles() removedDataFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedDataFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedDataFiles(@Nullable CounterResult value) {
    if (this.removedDataFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        value,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalDataFiles() totalDataFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDataFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalDataFiles(@Nullable CounterResult value) {
    if (this.totalDataFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        value,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedDeleteFiles() addedDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedDeleteFiles(@Nullable CounterResult value) {
    if (this.addedDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        value,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedEqualityDeleteFiles() addedEqualityDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedEqualityDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedEqualityDeleteFiles(@Nullable CounterResult value) {
    if (this.addedEqualityDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        value,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedPositionalDeleteFiles() addedPositionalDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedPositionalDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedPositionalDeleteFiles(@Nullable CounterResult value) {
    if (this.addedPositionalDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        value,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedDeleteFiles() removedDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedDeleteFiles(@Nullable CounterResult value) {
    if (this.removedDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        value,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedEqualityDeleteFiles() removedEqualityDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedEqualityDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedEqualityDeleteFiles(@Nullable CounterResult value) {
    if (this.removedEqualityDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        value,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedPositionalDeleteFiles() removedPositionalDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedPositionalDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedPositionalDeleteFiles(@Nullable CounterResult value) {
    if (this.removedPositionalDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        value,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalDeleteFiles() totalDeleteFiles} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDeleteFiles (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalDeleteFiles(@Nullable CounterResult value) {
    if (this.totalDeleteFiles == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        value,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedRecords() addedRecords} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedRecords (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedRecords(@Nullable CounterResult value) {
    if (this.addedRecords == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        value,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedRecords() removedRecords} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedRecords (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedRecords(@Nullable CounterResult value) {
    if (this.removedRecords == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        value,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalRecords() totalRecords} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalRecords (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalRecords(@Nullable CounterResult value) {
    if (this.totalRecords == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        value,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedFilesSizeInBytes() addedFilesSizeInBytes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedFilesSizeInBytes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedFilesSizeInBytes(@Nullable CounterResult value) {
    if (this.addedFilesSizeInBytes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        value,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedFilesSizeInBytes() removedFilesSizeInBytes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedFilesSizeInBytes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedFilesSizeInBytes(@Nullable CounterResult value) {
    if (this.removedFilesSizeInBytes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        value,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalFilesSizeInBytes() totalFilesSizeInBytes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalFilesSizeInBytes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalFilesSizeInBytes(@Nullable CounterResult value) {
    if (this.totalFilesSizeInBytes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        value,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedPositionalDeletes() addedPositionalDeletes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedPositionalDeletes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedPositionalDeletes(@Nullable CounterResult value) {
    if (this.addedPositionalDeletes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        value,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedPositionalDeletes() removedPositionalDeletes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedPositionalDeletes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedPositionalDeletes(@Nullable CounterResult value) {
    if (this.removedPositionalDeletes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        value,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalPositionalDeletes() totalPositionalDeletes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalPositionalDeletes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalPositionalDeletes(@Nullable CounterResult value) {
    if (this.totalPositionalDeletes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        value,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#addedEqualityDeletes() addedEqualityDeletes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addedEqualityDeletes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withAddedEqualityDeletes(@Nullable CounterResult value) {
    if (this.addedEqualityDeletes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        value,
        this.removedEqualityDeletes,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#removedEqualityDeletes() removedEqualityDeletes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for removedEqualityDeletes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withRemovedEqualityDeletes(@Nullable CounterResult value) {
    if (this.removedEqualityDeletes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        value,
        this.totalEqualityDeletes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetricsResult#totalEqualityDeletes() totalEqualityDeletes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalEqualityDeletes (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetricsResult withTotalEqualityDeletes(@Nullable CounterResult value) {
    if (this.totalEqualityDeletes == value) return this;
    return new ImmutableCommitMetricsResult(
        this.totalDuration,
        this.attempts,
        this.addedDataFiles,
        this.removedDataFiles,
        this.totalDataFiles,
        this.addedDeleteFiles,
        this.addedEqualityDeleteFiles,
        this.addedPositionalDeleteFiles,
        this.removedDeleteFiles,
        this.removedEqualityDeleteFiles,
        this.removedPositionalDeleteFiles,
        this.totalDeleteFiles,
        this.addedRecords,
        this.removedRecords,
        this.totalRecords,
        this.addedFilesSizeInBytes,
        this.removedFilesSizeInBytes,
        this.totalFilesSizeInBytes,
        this.addedPositionalDeletes,
        this.removedPositionalDeletes,
        this.totalPositionalDeletes,
        this.addedEqualityDeletes,
        this.removedEqualityDeletes,
        value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCommitMetricsResult} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCommitMetricsResult
        && equalTo(0, (ImmutableCommitMetricsResult) another);
  }

  private boolean equalTo(int synthetic, ImmutableCommitMetricsResult another) {
    return Objects.equals(totalDuration, another.totalDuration)
        && Objects.equals(attempts, another.attempts)
        && Objects.equals(addedDataFiles, another.addedDataFiles)
        && Objects.equals(removedDataFiles, another.removedDataFiles)
        && Objects.equals(totalDataFiles, another.totalDataFiles)
        && Objects.equals(addedDeleteFiles, another.addedDeleteFiles)
        && Objects.equals(addedEqualityDeleteFiles, another.addedEqualityDeleteFiles)
        && Objects.equals(addedPositionalDeleteFiles, another.addedPositionalDeleteFiles)
        && Objects.equals(removedDeleteFiles, another.removedDeleteFiles)
        && Objects.equals(removedEqualityDeleteFiles, another.removedEqualityDeleteFiles)
        && Objects.equals(removedPositionalDeleteFiles, another.removedPositionalDeleteFiles)
        && Objects.equals(totalDeleteFiles, another.totalDeleteFiles)
        && Objects.equals(addedRecords, another.addedRecords)
        && Objects.equals(removedRecords, another.removedRecords)
        && Objects.equals(totalRecords, another.totalRecords)
        && Objects.equals(addedFilesSizeInBytes, another.addedFilesSizeInBytes)
        && Objects.equals(removedFilesSizeInBytes, another.removedFilesSizeInBytes)
        && Objects.equals(totalFilesSizeInBytes, another.totalFilesSizeInBytes)
        && Objects.equals(addedPositionalDeletes, another.addedPositionalDeletes)
        && Objects.equals(removedPositionalDeletes, another.removedPositionalDeletes)
        && Objects.equals(totalPositionalDeletes, another.totalPositionalDeletes)
        && Objects.equals(addedEqualityDeletes, another.addedEqualityDeletes)
        && Objects.equals(removedEqualityDeletes, another.removedEqualityDeletes)
        && Objects.equals(totalEqualityDeletes, another.totalEqualityDeletes);
  }

  /**
   * Computes a hash code from attributes: {@code totalDuration}, {@code attempts}, {@code addedDataFiles}, {@code removedDataFiles}, {@code totalDataFiles}, {@code addedDeleteFiles}, {@code addedEqualityDeleteFiles}, {@code addedPositionalDeleteFiles}, {@code removedDeleteFiles}, {@code removedEqualityDeleteFiles}, {@code removedPositionalDeleteFiles}, {@code totalDeleteFiles}, {@code addedRecords}, {@code removedRecords}, {@code totalRecords}, {@code addedFilesSizeInBytes}, {@code removedFilesSizeInBytes}, {@code totalFilesSizeInBytes}, {@code addedPositionalDeletes}, {@code removedPositionalDeletes}, {@code totalPositionalDeletes}, {@code addedEqualityDeletes}, {@code removedEqualityDeletes}, {@code totalEqualityDeletes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(totalDuration);
    h += (h << 5) + Objects.hashCode(attempts);
    h += (h << 5) + Objects.hashCode(addedDataFiles);
    h += (h << 5) + Objects.hashCode(removedDataFiles);
    h += (h << 5) + Objects.hashCode(totalDataFiles);
    h += (h << 5) + Objects.hashCode(addedDeleteFiles);
    h += (h << 5) + Objects.hashCode(addedEqualityDeleteFiles);
    h += (h << 5) + Objects.hashCode(addedPositionalDeleteFiles);
    h += (h << 5) + Objects.hashCode(removedDeleteFiles);
    h += (h << 5) + Objects.hashCode(removedEqualityDeleteFiles);
    h += (h << 5) + Objects.hashCode(removedPositionalDeleteFiles);
    h += (h << 5) + Objects.hashCode(totalDeleteFiles);
    h += (h << 5) + Objects.hashCode(addedRecords);
    h += (h << 5) + Objects.hashCode(removedRecords);
    h += (h << 5) + Objects.hashCode(totalRecords);
    h += (h << 5) + Objects.hashCode(addedFilesSizeInBytes);
    h += (h << 5) + Objects.hashCode(removedFilesSizeInBytes);
    h += (h << 5) + Objects.hashCode(totalFilesSizeInBytes);
    h += (h << 5) + Objects.hashCode(addedPositionalDeletes);
    h += (h << 5) + Objects.hashCode(removedPositionalDeletes);
    h += (h << 5) + Objects.hashCode(totalPositionalDeletes);
    h += (h << 5) + Objects.hashCode(addedEqualityDeletes);
    h += (h << 5) + Objects.hashCode(removedEqualityDeletes);
    h += (h << 5) + Objects.hashCode(totalEqualityDeletes);
    return h;
  }

  /**
   * Prints the immutable value {@code CommitMetricsResult} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CommitMetricsResult{"
        + "totalDuration=" + totalDuration
        + ", attempts=" + attempts
        + ", addedDataFiles=" + addedDataFiles
        + ", removedDataFiles=" + removedDataFiles
        + ", totalDataFiles=" + totalDataFiles
        + ", addedDeleteFiles=" + addedDeleteFiles
        + ", addedEqualityDeleteFiles=" + addedEqualityDeleteFiles
        + ", addedPositionalDeleteFiles=" + addedPositionalDeleteFiles
        + ", removedDeleteFiles=" + removedDeleteFiles
        + ", removedEqualityDeleteFiles=" + removedEqualityDeleteFiles
        + ", removedPositionalDeleteFiles=" + removedPositionalDeleteFiles
        + ", totalDeleteFiles=" + totalDeleteFiles
        + ", addedRecords=" + addedRecords
        + ", removedRecords=" + removedRecords
        + ", totalRecords=" + totalRecords
        + ", addedFilesSizeInBytes=" + addedFilesSizeInBytes
        + ", removedFilesSizeInBytes=" + removedFilesSizeInBytes
        + ", totalFilesSizeInBytes=" + totalFilesSizeInBytes
        + ", addedPositionalDeletes=" + addedPositionalDeletes
        + ", removedPositionalDeletes=" + removedPositionalDeletes
        + ", totalPositionalDeletes=" + totalPositionalDeletes
        + ", addedEqualityDeletes=" + addedEqualityDeletes
        + ", removedEqualityDeletes=" + removedEqualityDeletes
        + ", totalEqualityDeletes=" + totalEqualityDeletes
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link CommitMetricsResult} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CommitMetricsResult instance
   */
  public static ImmutableCommitMetricsResult copyOf(CommitMetricsResult instance) {
    if (instance instanceof ImmutableCommitMetricsResult) {
      return (ImmutableCommitMetricsResult) instance;
    }
    return ImmutableCommitMetricsResult.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCommitMetricsResult ImmutableCommitMetricsResult}.
   * <pre>
   * ImmutableCommitMetricsResult.builder()
   *    .totalDuration(org.apache.iceberg.metrics.TimerResult | null) // nullable {@link CommitMetricsResult#totalDuration() totalDuration}
   *    .attempts(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#attempts() attempts}
   *    .addedDataFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedDataFiles() addedDataFiles}
   *    .removedDataFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedDataFiles() removedDataFiles}
   *    .totalDataFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#totalDataFiles() totalDataFiles}
   *    .addedDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedDeleteFiles() addedDeleteFiles}
   *    .addedEqualityDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedEqualityDeleteFiles() addedEqualityDeleteFiles}
   *    .addedPositionalDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedPositionalDeleteFiles() addedPositionalDeleteFiles}
   *    .removedDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedDeleteFiles() removedDeleteFiles}
   *    .removedEqualityDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedEqualityDeleteFiles() removedEqualityDeleteFiles}
   *    .removedPositionalDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedPositionalDeleteFiles() removedPositionalDeleteFiles}
   *    .totalDeleteFiles(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#totalDeleteFiles() totalDeleteFiles}
   *    .addedRecords(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedRecords() addedRecords}
   *    .removedRecords(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedRecords() removedRecords}
   *    .totalRecords(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#totalRecords() totalRecords}
   *    .addedFilesSizeInBytes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedFilesSizeInBytes() addedFilesSizeInBytes}
   *    .removedFilesSizeInBytes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedFilesSizeInBytes() removedFilesSizeInBytes}
   *    .totalFilesSizeInBytes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#totalFilesSizeInBytes() totalFilesSizeInBytes}
   *    .addedPositionalDeletes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedPositionalDeletes() addedPositionalDeletes}
   *    .removedPositionalDeletes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedPositionalDeletes() removedPositionalDeletes}
   *    .totalPositionalDeletes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#totalPositionalDeletes() totalPositionalDeletes}
   *    .addedEqualityDeletes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#addedEqualityDeletes() addedEqualityDeletes}
   *    .removedEqualityDeletes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#removedEqualityDeletes() removedEqualityDeletes}
   *    .totalEqualityDeletes(org.apache.iceberg.metrics.CounterResult | null) // nullable {@link CommitMetricsResult#totalEqualityDeletes() totalEqualityDeletes}
   *    .build();
   * </pre>
   * @return A new ImmutableCommitMetricsResult builder
   */
  public static ImmutableCommitMetricsResult.Builder builder() {
    return new ImmutableCommitMetricsResult.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCommitMetricsResult ImmutableCommitMetricsResult}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CommitMetricsResult", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private @Nullable TimerResult totalDuration;
    private @Nullable CounterResult attempts;
    private @Nullable CounterResult addedDataFiles;
    private @Nullable CounterResult removedDataFiles;
    private @Nullable CounterResult totalDataFiles;
    private @Nullable CounterResult addedDeleteFiles;
    private @Nullable CounterResult addedEqualityDeleteFiles;
    private @Nullable CounterResult addedPositionalDeleteFiles;
    private @Nullable CounterResult removedDeleteFiles;
    private @Nullable CounterResult removedEqualityDeleteFiles;
    private @Nullable CounterResult removedPositionalDeleteFiles;
    private @Nullable CounterResult totalDeleteFiles;
    private @Nullable CounterResult addedRecords;
    private @Nullable CounterResult removedRecords;
    private @Nullable CounterResult totalRecords;
    private @Nullable CounterResult addedFilesSizeInBytes;
    private @Nullable CounterResult removedFilesSizeInBytes;
    private @Nullable CounterResult totalFilesSizeInBytes;
    private @Nullable CounterResult addedPositionalDeletes;
    private @Nullable CounterResult removedPositionalDeletes;
    private @Nullable CounterResult totalPositionalDeletes;
    private @Nullable CounterResult addedEqualityDeletes;
    private @Nullable CounterResult removedEqualityDeletes;
    private @Nullable CounterResult totalEqualityDeletes;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CommitMetricsResult} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CommitMetricsResult instance) {
      Objects.requireNonNull(instance, "instance");
      @Nullable TimerResult totalDurationValue = instance.totalDuration();
      if (totalDurationValue != null) {
        totalDuration(totalDurationValue);
      }
      @Nullable CounterResult attemptsValue = instance.attempts();
      if (attemptsValue != null) {
        attempts(attemptsValue);
      }
      @Nullable CounterResult addedDataFilesValue = instance.addedDataFiles();
      if (addedDataFilesValue != null) {
        addedDataFiles(addedDataFilesValue);
      }
      @Nullable CounterResult removedDataFilesValue = instance.removedDataFiles();
      if (removedDataFilesValue != null) {
        removedDataFiles(removedDataFilesValue);
      }
      @Nullable CounterResult totalDataFilesValue = instance.totalDataFiles();
      if (totalDataFilesValue != null) {
        totalDataFiles(totalDataFilesValue);
      }
      @Nullable CounterResult addedDeleteFilesValue = instance.addedDeleteFiles();
      if (addedDeleteFilesValue != null) {
        addedDeleteFiles(addedDeleteFilesValue);
      }
      @Nullable CounterResult addedEqualityDeleteFilesValue = instance.addedEqualityDeleteFiles();
      if (addedEqualityDeleteFilesValue != null) {
        addedEqualityDeleteFiles(addedEqualityDeleteFilesValue);
      }
      @Nullable CounterResult addedPositionalDeleteFilesValue = instance.addedPositionalDeleteFiles();
      if (addedPositionalDeleteFilesValue != null) {
        addedPositionalDeleteFiles(addedPositionalDeleteFilesValue);
      }
      @Nullable CounterResult removedDeleteFilesValue = instance.removedDeleteFiles();
      if (removedDeleteFilesValue != null) {
        removedDeleteFiles(removedDeleteFilesValue);
      }
      @Nullable CounterResult removedEqualityDeleteFilesValue = instance.removedEqualityDeleteFiles();
      if (removedEqualityDeleteFilesValue != null) {
        removedEqualityDeleteFiles(removedEqualityDeleteFilesValue);
      }
      @Nullable CounterResult removedPositionalDeleteFilesValue = instance.removedPositionalDeleteFiles();
      if (removedPositionalDeleteFilesValue != null) {
        removedPositionalDeleteFiles(removedPositionalDeleteFilesValue);
      }
      @Nullable CounterResult totalDeleteFilesValue = instance.totalDeleteFiles();
      if (totalDeleteFilesValue != null) {
        totalDeleteFiles(totalDeleteFilesValue);
      }
      @Nullable CounterResult addedRecordsValue = instance.addedRecords();
      if (addedRecordsValue != null) {
        addedRecords(addedRecordsValue);
      }
      @Nullable CounterResult removedRecordsValue = instance.removedRecords();
      if (removedRecordsValue != null) {
        removedRecords(removedRecordsValue);
      }
      @Nullable CounterResult totalRecordsValue = instance.totalRecords();
      if (totalRecordsValue != null) {
        totalRecords(totalRecordsValue);
      }
      @Nullable CounterResult addedFilesSizeInBytesValue = instance.addedFilesSizeInBytes();
      if (addedFilesSizeInBytesValue != null) {
        addedFilesSizeInBytes(addedFilesSizeInBytesValue);
      }
      @Nullable CounterResult removedFilesSizeInBytesValue = instance.removedFilesSizeInBytes();
      if (removedFilesSizeInBytesValue != null) {
        removedFilesSizeInBytes(removedFilesSizeInBytesValue);
      }
      @Nullable CounterResult totalFilesSizeInBytesValue = instance.totalFilesSizeInBytes();
      if (totalFilesSizeInBytesValue != null) {
        totalFilesSizeInBytes(totalFilesSizeInBytesValue);
      }
      @Nullable CounterResult addedPositionalDeletesValue = instance.addedPositionalDeletes();
      if (addedPositionalDeletesValue != null) {
        addedPositionalDeletes(addedPositionalDeletesValue);
      }
      @Nullable CounterResult removedPositionalDeletesValue = instance.removedPositionalDeletes();
      if (removedPositionalDeletesValue != null) {
        removedPositionalDeletes(removedPositionalDeletesValue);
      }
      @Nullable CounterResult totalPositionalDeletesValue = instance.totalPositionalDeletes();
      if (totalPositionalDeletesValue != null) {
        totalPositionalDeletes(totalPositionalDeletesValue);
      }
      @Nullable CounterResult addedEqualityDeletesValue = instance.addedEqualityDeletes();
      if (addedEqualityDeletesValue != null) {
        addedEqualityDeletes(addedEqualityDeletesValue);
      }
      @Nullable CounterResult removedEqualityDeletesValue = instance.removedEqualityDeletes();
      if (removedEqualityDeletesValue != null) {
        removedEqualityDeletes(removedEqualityDeletesValue);
      }
      @Nullable CounterResult totalEqualityDeletesValue = instance.totalEqualityDeletes();
      if (totalEqualityDeletesValue != null) {
        totalEqualityDeletes(totalEqualityDeletesValue);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalDuration() totalDuration} attribute.
     * @param totalDuration The value for totalDuration (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDuration(@Nullable TimerResult totalDuration) {
      this.totalDuration = totalDuration;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#attempts() attempts} attribute.
     * @param attempts The value for attempts (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder attempts(@Nullable CounterResult attempts) {
      this.attempts = attempts;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedDataFiles() addedDataFiles} attribute.
     * @param addedDataFiles The value for addedDataFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedDataFiles(@Nullable CounterResult addedDataFiles) {
      this.addedDataFiles = addedDataFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedDataFiles() removedDataFiles} attribute.
     * @param removedDataFiles The value for removedDataFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedDataFiles(@Nullable CounterResult removedDataFiles) {
      this.removedDataFiles = removedDataFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalDataFiles() totalDataFiles} attribute.
     * @param totalDataFiles The value for totalDataFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDataFiles(@Nullable CounterResult totalDataFiles) {
      this.totalDataFiles = totalDataFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedDeleteFiles() addedDeleteFiles} attribute.
     * @param addedDeleteFiles The value for addedDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedDeleteFiles(@Nullable CounterResult addedDeleteFiles) {
      this.addedDeleteFiles = addedDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedEqualityDeleteFiles() addedEqualityDeleteFiles} attribute.
     * @param addedEqualityDeleteFiles The value for addedEqualityDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedEqualityDeleteFiles(@Nullable CounterResult addedEqualityDeleteFiles) {
      this.addedEqualityDeleteFiles = addedEqualityDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedPositionalDeleteFiles() addedPositionalDeleteFiles} attribute.
     * @param addedPositionalDeleteFiles The value for addedPositionalDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedPositionalDeleteFiles(@Nullable CounterResult addedPositionalDeleteFiles) {
      this.addedPositionalDeleteFiles = addedPositionalDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedDeleteFiles() removedDeleteFiles} attribute.
     * @param removedDeleteFiles The value for removedDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedDeleteFiles(@Nullable CounterResult removedDeleteFiles) {
      this.removedDeleteFiles = removedDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedEqualityDeleteFiles() removedEqualityDeleteFiles} attribute.
     * @param removedEqualityDeleteFiles The value for removedEqualityDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedEqualityDeleteFiles(@Nullable CounterResult removedEqualityDeleteFiles) {
      this.removedEqualityDeleteFiles = removedEqualityDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedPositionalDeleteFiles() removedPositionalDeleteFiles} attribute.
     * @param removedPositionalDeleteFiles The value for removedPositionalDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedPositionalDeleteFiles(@Nullable CounterResult removedPositionalDeleteFiles) {
      this.removedPositionalDeleteFiles = removedPositionalDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalDeleteFiles() totalDeleteFiles} attribute.
     * @param totalDeleteFiles The value for totalDeleteFiles (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDeleteFiles(@Nullable CounterResult totalDeleteFiles) {
      this.totalDeleteFiles = totalDeleteFiles;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedRecords() addedRecords} attribute.
     * @param addedRecords The value for addedRecords (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedRecords(@Nullable CounterResult addedRecords) {
      this.addedRecords = addedRecords;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedRecords() removedRecords} attribute.
     * @param removedRecords The value for removedRecords (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedRecords(@Nullable CounterResult removedRecords) {
      this.removedRecords = removedRecords;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalRecords() totalRecords} attribute.
     * @param totalRecords The value for totalRecords (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalRecords(@Nullable CounterResult totalRecords) {
      this.totalRecords = totalRecords;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedFilesSizeInBytes() addedFilesSizeInBytes} attribute.
     * @param addedFilesSizeInBytes The value for addedFilesSizeInBytes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedFilesSizeInBytes(@Nullable CounterResult addedFilesSizeInBytes) {
      this.addedFilesSizeInBytes = addedFilesSizeInBytes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedFilesSizeInBytes() removedFilesSizeInBytes} attribute.
     * @param removedFilesSizeInBytes The value for removedFilesSizeInBytes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedFilesSizeInBytes(@Nullable CounterResult removedFilesSizeInBytes) {
      this.removedFilesSizeInBytes = removedFilesSizeInBytes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalFilesSizeInBytes() totalFilesSizeInBytes} attribute.
     * @param totalFilesSizeInBytes The value for totalFilesSizeInBytes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalFilesSizeInBytes(@Nullable CounterResult totalFilesSizeInBytes) {
      this.totalFilesSizeInBytes = totalFilesSizeInBytes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedPositionalDeletes() addedPositionalDeletes} attribute.
     * @param addedPositionalDeletes The value for addedPositionalDeletes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedPositionalDeletes(@Nullable CounterResult addedPositionalDeletes) {
      this.addedPositionalDeletes = addedPositionalDeletes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedPositionalDeletes() removedPositionalDeletes} attribute.
     * @param removedPositionalDeletes The value for removedPositionalDeletes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedPositionalDeletes(@Nullable CounterResult removedPositionalDeletes) {
      this.removedPositionalDeletes = removedPositionalDeletes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalPositionalDeletes() totalPositionalDeletes} attribute.
     * @param totalPositionalDeletes The value for totalPositionalDeletes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalPositionalDeletes(@Nullable CounterResult totalPositionalDeletes) {
      this.totalPositionalDeletes = totalPositionalDeletes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#addedEqualityDeletes() addedEqualityDeletes} attribute.
     * @param addedEqualityDeletes The value for addedEqualityDeletes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addedEqualityDeletes(@Nullable CounterResult addedEqualityDeletes) {
      this.addedEqualityDeletes = addedEqualityDeletes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#removedEqualityDeletes() removedEqualityDeletes} attribute.
     * @param removedEqualityDeletes The value for removedEqualityDeletes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder removedEqualityDeletes(@Nullable CounterResult removedEqualityDeletes) {
      this.removedEqualityDeletes = removedEqualityDeletes;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetricsResult#totalEqualityDeletes() totalEqualityDeletes} attribute.
     * @param totalEqualityDeletes The value for totalEqualityDeletes (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalEqualityDeletes(@Nullable CounterResult totalEqualityDeletes) {
      this.totalEqualityDeletes = totalEqualityDeletes;
      return this;
    }

    /**
     * Builds a new {@link ImmutableCommitMetricsResult ImmutableCommitMetricsResult}.
     * @return An immutable instance of CommitMetricsResult
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCommitMetricsResult build() {
      return new ImmutableCommitMetricsResult(
          totalDuration,
          attempts,
          addedDataFiles,
          removedDataFiles,
          totalDataFiles,
          addedDeleteFiles,
          addedEqualityDeleteFiles,
          addedPositionalDeleteFiles,
          removedDeleteFiles,
          removedEqualityDeleteFiles,
          removedPositionalDeleteFiles,
          totalDeleteFiles,
          addedRecords,
          removedRecords,
          totalRecords,
          addedFilesSizeInBytes,
          removedFilesSizeInBytes,
          totalFilesSizeInBytes,
          addedPositionalDeletes,
          removedPositionalDeletes,
          totalPositionalDeletes,
          addedEqualityDeletes,
          removedEqualityDeletes,
          totalEqualityDeletes);
    }
  }
}
