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
package org.apache.iceberg.variants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Microbenchmark of Variant serialization, looking at shredding and nesting.
 *
 * <pre>
 * ./gradlew :iceberg-core:jmh  -PjmhIncludeRegex=VariantSerializationBenchmark
 * </pre>
 *
 * <p>Benchmark query: is variant ser/deser fast and does everything scale OK?
 *
 * <p>In particular: does shredding or nesting create problems?
 *
 * <p>Benchmark design: measure time to build, serialize and deserialize a variant object where the
 * dept of nesting and percentage shedding are parameterized.
 *
 * <p>Include different variant value types.
 *
 * <p>Within each benchmark method, repeat the operation {@link #ITERATIONS} time to compensate low
 * clock resolution on ARM cores. Real benchmarks MUST be performed on x86 parts so that {@code
 * rdtscp} is used to measure duration at nanosecond granularity.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 50)
@Measurement(iterations = 100)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class VariantSerializationBenchmark {

  /** How to nest the variant. */
  public enum Depth {
    /** Flat structure: no nesting. */
    Flat,
    /** Nested values. */
    Nested,
    /** Deeper nesting structure. */
    DeepNesting
  }

  private static final Logger LOG = LoggerFactory.getLogger(VariantSerializationBenchmark.class);

  /** Number of fields in each nested variant object. */
  private static final int NESTED_FIELD_COUNT = 5;

  /** On a deep nesting, how deep? */
  public static final int DEEP_NEST_DEPTH = 3;

  /**
   * How many iterations of each operation within a benchmark function? This is to compensate for
   * the lack of resolution of ARM CPUs, which only have a 50 MHz clock for measurement. Each method
   * has to iterate internally enought that the clock resolution doesn't affect the results.
   */
  public static final int ITERATIONS = 100;

  /** Percentage of fields that are shredded (0 = none, 100 = all). */
  @Param({"0", "33", "67", "100"})
  private int shreddedPercent;

  /**
   * Total number of fields in the variant object. Must be at least as big as {@link
   * #NESTED_FIELD_COUNT}.
   */
  @Param({"100", "500"})
  private int fieldCount;

  /** Depth of variant.. */
  @Param({"Flat", "Nested", "DeepNesting"})
  private Depth depth;

  /** Metadata covering every field name (shredded + unshredded). */
  private VariantMetadata metadata;

  /** Ordered list of all field names. */
  private List<String> fieldNames;

  /** Typed values for each field. Index i corresponds to {@code fieldNames.get(i)}. */
  private VariantValue[] fieldValues;

  /**
   * Number of fields that are placed into the shredded portion of the object, derived from {@link
   * #shreddedPercent} and {@link #fieldCount}.
   */
  private int shreddedFieldCount;

  /**
   * Pre-serialized object containing the unshredded fields, or {@code null} when all fields are
   * shredded.
   */
  private VariantObject unshreddedObject;

  /** Recycled output buffer. Must be large enough for the largest value of {@link #fieldCount}. */
  private ByteBuffer outputBuffer;

  /** For benchmarking cost of writing, ignoring creation cost. */
  private ShreddedObject preShreddedObject;

  /** Pre-shredded object marshalled to a byte buffer. */
  private ByteBuffer marshalledVariant;

  /** Drives choice of field types and more. */
  private Random random;

  /**
   * Set up the benchmark state for the current parameter combination.
   *
   * <p>Builds {@link #fieldNames} and {@link #fieldValues} with a random mix of ints, doubles,
   * strings of different sizes, and UUIDs. When nested, null values are patched with nested variant
   * objects containing string fields. The shredded/unshredded split and pre-serialized buffers are
   * then constructed from these values.
   */
  @Setup(Level.Trial)
  public void setupTrial() {
    LOG.info(
        "Setting up benchmark shreddedPercent={}% fields={} depth={}",
        shreddedPercent, fieldCount, depth);
    // fixed random for reproducibility; other benchmarks are a mix of fixed and time-based seeds.
    random = new Random(0x1ceb1cebL);

    fieldNames = Lists.newArrayListWithCapacity(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      fieldNames.add("field_" + i);
    }
    ByteBuffer metaBuf = VariantTestUtil.createMetadata(fieldNames, true /* sorted */);
    metadata = VariantMetadata.from(metaBuf);
    final boolean nested = depth != Depth.Flat;

    // construct the field values such that any nested variants are composed of strings
    // with the same field names as the top-level entries.
    List<Integer> nullFields = Lists.newArrayList();
    List<Integer> stringFields = Lists.newArrayList();

    // a factory which creates variants; by adding more string functions
    // it is biased towards strings.
    List<Function<Integer, VariantValue>> variantFactory = Lists.newArrayList();
    variantFactory.add(i -> Variants.of("string-" + i));
    variantFactory.add(i -> Variants.of("longer string-" + i));
    variantFactory.add(
        i -> Variants.of("a longer string assuming these will be more common #" + i));
    variantFactory.add(i -> Variants.of('a'));
    variantFactory.add(i -> Variants.of(random.nextInt()));
    variantFactory.add(i -> Variants.of(random.nextDouble()));
    variantFactory.add(i -> Variants.of(random.nextBoolean()));
    // as an example of a byte sequence other than string.
    variantFactory.add(i -> Variants.ofUUID(UUID.randomUUID()));
    variantFactory.add(i -> Variants.of(new BigDecimal(new BigInteger(64, random))));
    if (nested) {
      // on a deep variant, this will be replaced by a nested type later
      variantFactory.add(i -> Variants.ofNull());
    } else {
      // add a string on shallow to keep the distribution of other values similar.
      // leaving the null made serialization faster.
      variantFactory.add(i -> Variants.of(Integer.toString(i)));
    }
    final int factorySize = variantFactory.size();
    fieldValues = new VariantValue[fieldCount];

    // build the field values.
    for (int i = 0; i < fieldCount; i++) {
      final VariantValue value = variantFactory.get(random.nextInt(factorySize)).apply(i);
      fieldValues[i] = value;
      if (value.type() == PhysicalType.STRING) {
        stringFields.add(i);
      } else if (value.type() == PhysicalType.NULL) {
        nullFields.add(i);
      }
    }

    // now, on a nested run patch null fields with a nested variant
    if (nested) {
      Preconditions.checkState(!stringFields.isEmpty(), "No string fields generated");
      nullFields.forEach(
          index -> fieldValues[index] = buildNestedValue(index, stringFields, DEEP_NEST_DEPTH));
    }

    // set up the shredding
    shreddedFieldCount = fieldCount * shreddedPercent / 100;
    if (shreddedFieldCount < fieldCount) {
      unshreddedObject = buildSerializedObject(shreddedFieldCount, fieldCount, metaBuf);
    } else {
      unshreddedObject = null;
    }

    // build a pre-shredded object for serialization only tests
    preShreddedObject = buildShreddedObject();
    // use its size for buffer allocation
    final int size = preShreddedObject.sizeInBytes();
    // this buffer is recycled in benchmarks to avoid memory access interference
    outputBuffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);

    // a marshalled object to measure deserialization performance
    marshalledVariant = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    preShreddedObject.writeTo(marshalledVariant, 0);

    LOG.info("Setup complete");
  }

  /**
   * Build a nested variant object containing {@link #NESTED_FIELD_COUNT} string fields, reusing
   * field names from the top-level field list.
   *
   * @param index index of row
   * @param stringFields list of fields which are strings
   * @param level levels to descend.
   */
  private VariantValue buildNestedValue(int index, List<Integer> stringFields, int level) {
    final ShreddedObject entry = buildSingleNestedEntry(index, stringFields);
    if (depth == Depth.DeepNesting && level > 0) {
      // DeepNesting: outer object whose fields are themselves nested objects (two levels deep)
      entry.put(
          "nested_" + (NESTED_FIELD_COUNT + 1), buildNestedValue(index, stringFields, level - 1));
    }

    return entry;
  }

  /**
   * Build a single nested entry.
   *
   * @param index row index. list of fields which are strings.
   * @param stringFields list of fields which are strings
   * @return a nested entry.
   */
  private ShreddedObject buildSingleNestedEntry(int index, List<Integer> stringFields) {
    ShreddedObject nested = Variants.object(metadata);
    final int stringCount = stringFields.size();

    for (int j = 0; j < NESTED_FIELD_COUNT; j++) {
      nested.put(
          fieldNames.get(stringFields.get(random.nextInt(stringCount))),
          Variants.of("nested_" + index + "_" + j));
    }
    return nested;
  }

  /**
   * Serializes a subset of fields into a {@link VariantObject} backed by a {@link ByteBuffer} so it
   * can be used as the unshredded remainder.
   *
   * @param from inclusive start index into {@link #fieldNames}
   * @param to exclusive end index into {@link #fieldNames}
   * @param metaBuf the shared metadata buffer
   */
  private VariantObject buildSerializedObject(int from, int to, ByteBuffer metaBuf) {
    LOG.info("serialize {}-{}", from, to);
    ImmutableMap.Builder<String, VariantValue> builder = ImmutableMap.builder();
    for (int i = from; i < to; i++) {
      builder.put(fieldNames.get(i), fieldValues[i]);
    }
    Map<String, VariantValue> fields = builder.build();
    ByteBuffer valueBuf = VariantTestUtil.createObject(metaBuf, fields);
    return (VariantObject) Variants.value(metadata, valueBuf);
  }

  /**
   * Build a shredded object from the benchmark's current fields.
   *
   * @return a new shredded object.
   */
  private ShreddedObject buildShreddedObject() {
    ShreddedObject shredded = Variants.object(metadata, unshreddedObject);

    for (int i = 0; i < shreddedFieldCount; i++) {
      shredded.put(fieldNames.get(i), fieldValues[i]);
    }
    return shredded;
  }

  /**
   * Serialize-only path: reuse a pre-built {@link ShreddedObject} and measure the cost of {@link
   * ShreddedObject#sizeInBytes()} and {@link ShreddedObject#writeTo(ByteBuffer, int)}.
   *
   * <p>The output buffer is recycled to isolate serialization cost from memory allocation.
   */
  @Benchmark
  public void serialize(Blackhole bh) {
    for (int i = 0; i < ITERATIONS; i++) {
      outputBuffer.clear();
      bh.consume(preShreddedObject.sizeInBytes());
      preShreddedObject.writeTo(outputBuffer, 0);
      bh.consume(outputBuffer);
    }
  }

  /**
   * Deserialize-only path: read a pre-serialized variant from a memory buffer and access every
   * field.
   */
  @Benchmark
  public void deserialize(Blackhole bh) {
    for (int i = 0; i < ITERATIONS; i++) {
      marshalledVariant.rewind();
      VariantObject parsed = (VariantObject) Variants.value(metadata, marshalledVariant);
      for (String name : fieldNames) {
        bh.consume(parsed.get(name));
      }
    }
  }

  /**
   * Round-trip: serialize a pre-built {@link ShreddedObject} into a recycled memory buffer, then
   * immediately deserialize from that same buffer and access every field.
   *
   * <p>This is the most realistic benchmark: it exercises the full serialize → write → read →
   * deserialize path that production code follows, with the buffer reused to focus measurement on
   * the variant logic rather than memory allocation.
   */
  @Benchmark
  public void roundTrip(Blackhole bh) {
    for (int i = 0; i < ITERATIONS; i++) {
      outputBuffer.clear();
      bh.consume(preShreddedObject.sizeInBytes());
      preShreddedObject.writeTo(outputBuffer, 0);
      VariantObject parsed = (VariantObject) Variants.value(metadata, outputBuffer);
      for (String name : fieldNames) {
        bh.consume(parsed.get(name));
      }
    }
  }
}
