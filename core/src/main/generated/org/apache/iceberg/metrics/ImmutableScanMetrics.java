package org.apache.iceberg.metrics;

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
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ScanMetrics}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableScanMetrics.builder()}.
 */
@Generated(from = "ScanMetrics", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableScanMetrics extends ScanMetrics {
  private final MetricsContext metricsContext;
  private transient final Timer totalPlanningDuration;
  private transient final Counter resultDataFiles;
  private transient final Counter resultDeleteFiles;
  private transient final Counter scannedDataManifests;
  private transient final Counter totalDataManifests;
  private transient final Counter totalDeleteManifests;
  private transient final Counter totalFileSizeInBytes;
  private transient final Counter totalDeleteFileSizeInBytes;
  private transient final Counter skippedDataManifests;
  private transient final Counter skippedDataFiles;
  private transient final Counter skippedDeleteFiles;
  private transient final Counter scannedDeleteManifests;
  private transient final Counter skippedDeleteManifests;
  private transient final Counter indexedDeleteFiles;
  private transient final Counter equalityDeleteFiles;
  private transient final Counter positionalDeleteFiles;

  private ImmutableScanMetrics(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
    this.totalPlanningDuration = initShim.totalPlanningDuration();
    this.resultDataFiles = initShim.resultDataFiles();
    this.resultDeleteFiles = initShim.resultDeleteFiles();
    this.scannedDataManifests = initShim.scannedDataManifests();
    this.totalDataManifests = initShim.totalDataManifests();
    this.totalDeleteManifests = initShim.totalDeleteManifests();
    this.totalFileSizeInBytes = initShim.totalFileSizeInBytes();
    this.totalDeleteFileSizeInBytes = initShim.totalDeleteFileSizeInBytes();
    this.skippedDataManifests = initShim.skippedDataManifests();
    this.skippedDataFiles = initShim.skippedDataFiles();
    this.skippedDeleteFiles = initShim.skippedDeleteFiles();
    this.scannedDeleteManifests = initShim.scannedDeleteManifests();
    this.skippedDeleteManifests = initShim.skippedDeleteManifests();
    this.indexedDeleteFiles = initShim.indexedDeleteFiles();
    this.equalityDeleteFiles = initShim.equalityDeleteFiles();
    this.positionalDeleteFiles = initShim.positionalDeleteFiles();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ScanMetrics", generator = "Immutables")
  private final class InitShim {
    private byte totalPlanningDurationBuildStage = STAGE_UNINITIALIZED;
    private Timer totalPlanningDuration;

    Timer totalPlanningDuration() {
      if (totalPlanningDurationBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (totalPlanningDurationBuildStage == STAGE_UNINITIALIZED) {
        totalPlanningDurationBuildStage = STAGE_INITIALIZING;
        this.totalPlanningDuration = Objects.requireNonNull(ImmutableScanMetrics.super.totalPlanningDuration(), "totalPlanningDuration");
        totalPlanningDurationBuildStage = STAGE_INITIALIZED;
      }
      return this.totalPlanningDuration;
    }

    private byte resultDataFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter resultDataFiles;

    Counter resultDataFiles() {
      if (resultDataFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (resultDataFilesBuildStage == STAGE_UNINITIALIZED) {
        resultDataFilesBuildStage = STAGE_INITIALIZING;
        this.resultDataFiles = Objects.requireNonNull(ImmutableScanMetrics.super.resultDataFiles(), "resultDataFiles");
        resultDataFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.resultDataFiles;
    }

    private byte resultDeleteFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter resultDeleteFiles;

    Counter resultDeleteFiles() {
      if (resultDeleteFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (resultDeleteFilesBuildStage == STAGE_UNINITIALIZED) {
        resultDeleteFilesBuildStage = STAGE_INITIALIZING;
        this.resultDeleteFiles = Objects.requireNonNull(ImmutableScanMetrics.super.resultDeleteFiles(), "resultDeleteFiles");
        resultDeleteFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.resultDeleteFiles;
    }

    private byte scannedDataManifestsBuildStage = STAGE_UNINITIALIZED;
    private Counter scannedDataManifests;

    Counter scannedDataManifests() {
      if (scannedDataManifestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (scannedDataManifestsBuildStage == STAGE_UNINITIALIZED) {
        scannedDataManifestsBuildStage = STAGE_INITIALIZING;
        this.scannedDataManifests = Objects.requireNonNull(ImmutableScanMetrics.super.scannedDataManifests(), "scannedDataManifests");
        scannedDataManifestsBuildStage = STAGE_INITIALIZED;
      }
      return this.scannedDataManifests;
    }

    private byte totalDataManifestsBuildStage = STAGE_UNINITIALIZED;
    private Counter totalDataManifests;

    Counter totalDataManifests() {
      if (totalDataManifestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (totalDataManifestsBuildStage == STAGE_UNINITIALIZED) {
        totalDataManifestsBuildStage = STAGE_INITIALIZING;
        this.totalDataManifests = Objects.requireNonNull(ImmutableScanMetrics.super.totalDataManifests(), "totalDataManifests");
        totalDataManifestsBuildStage = STAGE_INITIALIZED;
      }
      return this.totalDataManifests;
    }

    private byte totalDeleteManifestsBuildStage = STAGE_UNINITIALIZED;
    private Counter totalDeleteManifests;

    Counter totalDeleteManifests() {
      if (totalDeleteManifestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (totalDeleteManifestsBuildStage == STAGE_UNINITIALIZED) {
        totalDeleteManifestsBuildStage = STAGE_INITIALIZING;
        this.totalDeleteManifests = Objects.requireNonNull(ImmutableScanMetrics.super.totalDeleteManifests(), "totalDeleteManifests");
        totalDeleteManifestsBuildStage = STAGE_INITIALIZED;
      }
      return this.totalDeleteManifests;
    }

    private byte totalFileSizeInBytesBuildStage = STAGE_UNINITIALIZED;
    private Counter totalFileSizeInBytes;

    Counter totalFileSizeInBytes() {
      if (totalFileSizeInBytesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (totalFileSizeInBytesBuildStage == STAGE_UNINITIALIZED) {
        totalFileSizeInBytesBuildStage = STAGE_INITIALIZING;
        this.totalFileSizeInBytes = Objects.requireNonNull(ImmutableScanMetrics.super.totalFileSizeInBytes(), "totalFileSizeInBytes");
        totalFileSizeInBytesBuildStage = STAGE_INITIALIZED;
      }
      return this.totalFileSizeInBytes;
    }

    private byte totalDeleteFileSizeInBytesBuildStage = STAGE_UNINITIALIZED;
    private Counter totalDeleteFileSizeInBytes;

    Counter totalDeleteFileSizeInBytes() {
      if (totalDeleteFileSizeInBytesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (totalDeleteFileSizeInBytesBuildStage == STAGE_UNINITIALIZED) {
        totalDeleteFileSizeInBytesBuildStage = STAGE_INITIALIZING;
        this.totalDeleteFileSizeInBytes = Objects.requireNonNull(ImmutableScanMetrics.super.totalDeleteFileSizeInBytes(), "totalDeleteFileSizeInBytes");
        totalDeleteFileSizeInBytesBuildStage = STAGE_INITIALIZED;
      }
      return this.totalDeleteFileSizeInBytes;
    }

    private byte skippedDataManifestsBuildStage = STAGE_UNINITIALIZED;
    private Counter skippedDataManifests;

    Counter skippedDataManifests() {
      if (skippedDataManifestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (skippedDataManifestsBuildStage == STAGE_UNINITIALIZED) {
        skippedDataManifestsBuildStage = STAGE_INITIALIZING;
        this.skippedDataManifests = Objects.requireNonNull(ImmutableScanMetrics.super.skippedDataManifests(), "skippedDataManifests");
        skippedDataManifestsBuildStage = STAGE_INITIALIZED;
      }
      return this.skippedDataManifests;
    }

    private byte skippedDataFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter skippedDataFiles;

    Counter skippedDataFiles() {
      if (skippedDataFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (skippedDataFilesBuildStage == STAGE_UNINITIALIZED) {
        skippedDataFilesBuildStage = STAGE_INITIALIZING;
        this.skippedDataFiles = Objects.requireNonNull(ImmutableScanMetrics.super.skippedDataFiles(), "skippedDataFiles");
        skippedDataFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.skippedDataFiles;
    }

    private byte skippedDeleteFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter skippedDeleteFiles;

    Counter skippedDeleteFiles() {
      if (skippedDeleteFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (skippedDeleteFilesBuildStage == STAGE_UNINITIALIZED) {
        skippedDeleteFilesBuildStage = STAGE_INITIALIZING;
        this.skippedDeleteFiles = Objects.requireNonNull(ImmutableScanMetrics.super.skippedDeleteFiles(), "skippedDeleteFiles");
        skippedDeleteFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.skippedDeleteFiles;
    }

    private byte scannedDeleteManifestsBuildStage = STAGE_UNINITIALIZED;
    private Counter scannedDeleteManifests;

    Counter scannedDeleteManifests() {
      if (scannedDeleteManifestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (scannedDeleteManifestsBuildStage == STAGE_UNINITIALIZED) {
        scannedDeleteManifestsBuildStage = STAGE_INITIALIZING;
        this.scannedDeleteManifests = Objects.requireNonNull(ImmutableScanMetrics.super.scannedDeleteManifests(), "scannedDeleteManifests");
        scannedDeleteManifestsBuildStage = STAGE_INITIALIZED;
      }
      return this.scannedDeleteManifests;
    }

    private byte skippedDeleteManifestsBuildStage = STAGE_UNINITIALIZED;
    private Counter skippedDeleteManifests;

    Counter skippedDeleteManifests() {
      if (skippedDeleteManifestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (skippedDeleteManifestsBuildStage == STAGE_UNINITIALIZED) {
        skippedDeleteManifestsBuildStage = STAGE_INITIALIZING;
        this.skippedDeleteManifests = Objects.requireNonNull(ImmutableScanMetrics.super.skippedDeleteManifests(), "skippedDeleteManifests");
        skippedDeleteManifestsBuildStage = STAGE_INITIALIZED;
      }
      return this.skippedDeleteManifests;
    }

    private byte indexedDeleteFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter indexedDeleteFiles;

    Counter indexedDeleteFiles() {
      if (indexedDeleteFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (indexedDeleteFilesBuildStage == STAGE_UNINITIALIZED) {
        indexedDeleteFilesBuildStage = STAGE_INITIALIZING;
        this.indexedDeleteFiles = Objects.requireNonNull(ImmutableScanMetrics.super.indexedDeleteFiles(), "indexedDeleteFiles");
        indexedDeleteFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.indexedDeleteFiles;
    }

    private byte equalityDeleteFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter equalityDeleteFiles;

    Counter equalityDeleteFiles() {
      if (equalityDeleteFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (equalityDeleteFilesBuildStage == STAGE_UNINITIALIZED) {
        equalityDeleteFilesBuildStage = STAGE_INITIALIZING;
        this.equalityDeleteFiles = Objects.requireNonNull(ImmutableScanMetrics.super.equalityDeleteFiles(), "equalityDeleteFiles");
        equalityDeleteFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.equalityDeleteFiles;
    }

    private byte positionalDeleteFilesBuildStage = STAGE_UNINITIALIZED;
    private Counter positionalDeleteFiles;

    Counter positionalDeleteFiles() {
      if (positionalDeleteFilesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (positionalDeleteFilesBuildStage == STAGE_UNINITIALIZED) {
        positionalDeleteFilesBuildStage = STAGE_INITIALIZING;
        this.positionalDeleteFiles = Objects.requireNonNull(ImmutableScanMetrics.super.positionalDeleteFiles(), "positionalDeleteFiles");
        positionalDeleteFilesBuildStage = STAGE_INITIALIZED;
      }
      return this.positionalDeleteFiles;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (totalPlanningDurationBuildStage == STAGE_INITIALIZING) attributes.add("totalPlanningDuration");
      if (resultDataFilesBuildStage == STAGE_INITIALIZING) attributes.add("resultDataFiles");
      if (resultDeleteFilesBuildStage == STAGE_INITIALIZING) attributes.add("resultDeleteFiles");
      if (scannedDataManifestsBuildStage == STAGE_INITIALIZING) attributes.add("scannedDataManifests");
      if (totalDataManifestsBuildStage == STAGE_INITIALIZING) attributes.add("totalDataManifests");
      if (totalDeleteManifestsBuildStage == STAGE_INITIALIZING) attributes.add("totalDeleteManifests");
      if (totalFileSizeInBytesBuildStage == STAGE_INITIALIZING) attributes.add("totalFileSizeInBytes");
      if (totalDeleteFileSizeInBytesBuildStage == STAGE_INITIALIZING) attributes.add("totalDeleteFileSizeInBytes");
      if (skippedDataManifestsBuildStage == STAGE_INITIALIZING) attributes.add("skippedDataManifests");
      if (skippedDataFilesBuildStage == STAGE_INITIALIZING) attributes.add("skippedDataFiles");
      if (skippedDeleteFilesBuildStage == STAGE_INITIALIZING) attributes.add("skippedDeleteFiles");
      if (scannedDeleteManifestsBuildStage == STAGE_INITIALIZING) attributes.add("scannedDeleteManifests");
      if (skippedDeleteManifestsBuildStage == STAGE_INITIALIZING) attributes.add("skippedDeleteManifests");
      if (indexedDeleteFilesBuildStage == STAGE_INITIALIZING) attributes.add("indexedDeleteFiles");
      if (equalityDeleteFilesBuildStage == STAGE_INITIALIZING) attributes.add("equalityDeleteFiles");
      if (positionalDeleteFilesBuildStage == STAGE_INITIALIZING) attributes.add("positionalDeleteFiles");
      return "Cannot build ScanMetrics, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code metricsContext} attribute
   */
  @Override
  public MetricsContext metricsContext() {
    return metricsContext;
  }

  /**
   * @return The computed-at-construction value of the {@code totalPlanningDuration} attribute
   */
  @Override
  public Timer totalPlanningDuration() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.totalPlanningDuration()
        : this.totalPlanningDuration;
  }

  /**
   * @return The computed-at-construction value of the {@code resultDataFiles} attribute
   */
  @Override
  public Counter resultDataFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.resultDataFiles()
        : this.resultDataFiles;
  }

  /**
   * @return The computed-at-construction value of the {@code resultDeleteFiles} attribute
   */
  @Override
  public Counter resultDeleteFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.resultDeleteFiles()
        : this.resultDeleteFiles;
  }

  /**
   * @return The computed-at-construction value of the {@code scannedDataManifests} attribute
   */
  @Override
  public Counter scannedDataManifests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.scannedDataManifests()
        : this.scannedDataManifests;
  }

  /**
   * @return The computed-at-construction value of the {@code totalDataManifests} attribute
   */
  @Override
  public Counter totalDataManifests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.totalDataManifests()
        : this.totalDataManifests;
  }

  /**
   * @return The computed-at-construction value of the {@code totalDeleteManifests} attribute
   */
  @Override
  public Counter totalDeleteManifests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.totalDeleteManifests()
        : this.totalDeleteManifests;
  }

  /**
   * @return The computed-at-construction value of the {@code totalFileSizeInBytes} attribute
   */
  @Override
  public Counter totalFileSizeInBytes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.totalFileSizeInBytes()
        : this.totalFileSizeInBytes;
  }

  /**
   * @return The computed-at-construction value of the {@code totalDeleteFileSizeInBytes} attribute
   */
  @Override
  public Counter totalDeleteFileSizeInBytes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.totalDeleteFileSizeInBytes()
        : this.totalDeleteFileSizeInBytes;
  }

  /**
   * @return The computed-at-construction value of the {@code skippedDataManifests} attribute
   */
  @Override
  public Counter skippedDataManifests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.skippedDataManifests()
        : this.skippedDataManifests;
  }

  /**
   * @return The computed-at-construction value of the {@code skippedDataFiles} attribute
   */
  @Override
  public Counter skippedDataFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.skippedDataFiles()
        : this.skippedDataFiles;
  }

  /**
   * @return The computed-at-construction value of the {@code skippedDeleteFiles} attribute
   */
  @Override
  public Counter skippedDeleteFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.skippedDeleteFiles()
        : this.skippedDeleteFiles;
  }

  /**
   * @return The computed-at-construction value of the {@code scannedDeleteManifests} attribute
   */
  @Override
  public Counter scannedDeleteManifests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.scannedDeleteManifests()
        : this.scannedDeleteManifests;
  }

  /**
   * @return The computed-at-construction value of the {@code skippedDeleteManifests} attribute
   */
  @Override
  public Counter skippedDeleteManifests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.skippedDeleteManifests()
        : this.skippedDeleteManifests;
  }

  /**
   * @return The computed-at-construction value of the {@code indexedDeleteFiles} attribute
   */
  @Override
  public Counter indexedDeleteFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.indexedDeleteFiles()
        : this.indexedDeleteFiles;
  }

  /**
   * @return The computed-at-construction value of the {@code equalityDeleteFiles} attribute
   */
  @Override
  public Counter equalityDeleteFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.equalityDeleteFiles()
        : this.equalityDeleteFiles;
  }

  /**
   * @return The computed-at-construction value of the {@code positionalDeleteFiles} attribute
   */
  @Override
  public Counter positionalDeleteFiles() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.positionalDeleteFiles()
        : this.positionalDeleteFiles;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanMetrics#metricsContext() metricsContext} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for metricsContext
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanMetrics withMetricsContext(MetricsContext value) {
    if (this.metricsContext == value) return this;
    MetricsContext newValue = Objects.requireNonNull(value, "metricsContext");
    return new ImmutableScanMetrics(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableScanMetrics} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableScanMetrics
        && equalTo(0, (ImmutableScanMetrics) another);
  }

  private boolean equalTo(int synthetic, ImmutableScanMetrics another) {
    return metricsContext.equals(another.metricsContext)
        && totalPlanningDuration.equals(another.totalPlanningDuration)
        && resultDataFiles.equals(another.resultDataFiles)
        && resultDeleteFiles.equals(another.resultDeleteFiles)
        && scannedDataManifests.equals(another.scannedDataManifests)
        && totalDataManifests.equals(another.totalDataManifests)
        && totalDeleteManifests.equals(another.totalDeleteManifests)
        && totalFileSizeInBytes.equals(another.totalFileSizeInBytes)
        && totalDeleteFileSizeInBytes.equals(another.totalDeleteFileSizeInBytes)
        && skippedDataManifests.equals(another.skippedDataManifests)
        && skippedDataFiles.equals(another.skippedDataFiles)
        && skippedDeleteFiles.equals(another.skippedDeleteFiles)
        && scannedDeleteManifests.equals(another.scannedDeleteManifests)
        && skippedDeleteManifests.equals(another.skippedDeleteManifests)
        && indexedDeleteFiles.equals(another.indexedDeleteFiles)
        && equalityDeleteFiles.equals(another.equalityDeleteFiles)
        && positionalDeleteFiles.equals(another.positionalDeleteFiles);
  }

  /**
   * Computes a hash code from attributes: {@code metricsContext}, {@code totalPlanningDuration}, {@code resultDataFiles}, {@code resultDeleteFiles}, {@code scannedDataManifests}, {@code totalDataManifests}, {@code totalDeleteManifests}, {@code totalFileSizeInBytes}, {@code totalDeleteFileSizeInBytes}, {@code skippedDataManifests}, {@code skippedDataFiles}, {@code skippedDeleteFiles}, {@code scannedDeleteManifests}, {@code skippedDeleteManifests}, {@code indexedDeleteFiles}, {@code equalityDeleteFiles}, {@code positionalDeleteFiles}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + metricsContext.hashCode();
    h += (h << 5) + totalPlanningDuration.hashCode();
    h += (h << 5) + resultDataFiles.hashCode();
    h += (h << 5) + resultDeleteFiles.hashCode();
    h += (h << 5) + scannedDataManifests.hashCode();
    h += (h << 5) + totalDataManifests.hashCode();
    h += (h << 5) + totalDeleteManifests.hashCode();
    h += (h << 5) + totalFileSizeInBytes.hashCode();
    h += (h << 5) + totalDeleteFileSizeInBytes.hashCode();
    h += (h << 5) + skippedDataManifests.hashCode();
    h += (h << 5) + skippedDataFiles.hashCode();
    h += (h << 5) + skippedDeleteFiles.hashCode();
    h += (h << 5) + scannedDeleteManifests.hashCode();
    h += (h << 5) + skippedDeleteManifests.hashCode();
    h += (h << 5) + indexedDeleteFiles.hashCode();
    h += (h << 5) + equalityDeleteFiles.hashCode();
    h += (h << 5) + positionalDeleteFiles.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ScanMetrics} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ScanMetrics{"
        + "metricsContext=" + metricsContext
        + ", totalPlanningDuration=" + totalPlanningDuration
        + ", resultDataFiles=" + resultDataFiles
        + ", resultDeleteFiles=" + resultDeleteFiles
        + ", scannedDataManifests=" + scannedDataManifests
        + ", totalDataManifests=" + totalDataManifests
        + ", totalDeleteManifests=" + totalDeleteManifests
        + ", totalFileSizeInBytes=" + totalFileSizeInBytes
        + ", totalDeleteFileSizeInBytes=" + totalDeleteFileSizeInBytes
        + ", skippedDataManifests=" + skippedDataManifests
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
   * Creates an immutable copy of a {@link ScanMetrics} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ScanMetrics instance
   */
  public static ImmutableScanMetrics copyOf(ScanMetrics instance) {
    if (instance instanceof ImmutableScanMetrics) {
      return (ImmutableScanMetrics) instance;
    }
    return ImmutableScanMetrics.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableScanMetrics ImmutableScanMetrics}.
   * <pre>
   * ImmutableScanMetrics.builder()
   *    .metricsContext(org.apache.iceberg.metrics.MetricsContext) // required {@link ScanMetrics#metricsContext() metricsContext}
   *    .build();
   * </pre>
   * @return A new ImmutableScanMetrics builder
   */
  public static ImmutableScanMetrics.Builder builder() {
    return new ImmutableScanMetrics.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableScanMetrics ImmutableScanMetrics}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ScanMetrics", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_METRICS_CONTEXT = 0x1L;
    private long initBits = 0x1L;

    private @Nullable MetricsContext metricsContext;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ScanMetrics} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ScanMetrics instance) {
      Objects.requireNonNull(instance, "instance");
      metricsContext(instance.metricsContext());
      return this;
    }

    /**
     * Initializes the value for the {@link ScanMetrics#metricsContext() metricsContext} attribute.
     * @param metricsContext The value for metricsContext 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder metricsContext(MetricsContext metricsContext) {
      this.metricsContext = Objects.requireNonNull(metricsContext, "metricsContext");
      initBits &= ~INIT_BIT_METRICS_CONTEXT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableScanMetrics ImmutableScanMetrics}.
     * @return An immutable instance of ScanMetrics
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableScanMetrics build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableScanMetrics(metricsContext);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_METRICS_CONTEXT) != 0) attributes.add("metricsContext");
      return "Cannot build ScanMetrics, some of required attributes are not set " + attributes;
    }
  }
}
