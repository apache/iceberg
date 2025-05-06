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
package org.apache.iceberg.types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;

public class Comparators {

  private Comparators() {}

  private static final ImmutableMap<Type.PrimitiveType, Comparator<?>> COMPARATORS =
      ImmutableMap.<Type.PrimitiveType, Comparator<?>>builder()
          .put(Types.BooleanType.get(), Comparator.naturalOrder())
          .put(Types.IntegerType.get(), Comparator.naturalOrder())
          .put(Types.LongType.get(), Comparator.naturalOrder())
          .put(Types.FloatType.get(), Comparator.naturalOrder())
          .put(Types.DoubleType.get(), Comparator.naturalOrder())
          .put(Types.DateType.get(), Comparator.naturalOrder())
          .put(Types.TimeType.get(), Comparator.naturalOrder())
          .put(Types.TimestampType.withZone(), Comparator.naturalOrder())
          .put(Types.TimestampType.withoutZone(), Comparator.naturalOrder())
          .put(Types.TimestampNanoType.withZone(), Comparator.naturalOrder())
          .put(Types.TimestampNanoType.withoutZone(), Comparator.naturalOrder())
          .put(Types.StringType.get(), Comparators.charSequences())
          .put(Types.UUIDType.get(), Comparator.naturalOrder())
          .put(Types.BinaryType.get(), Comparators.unsignedBytes())
          .buildOrThrow();

  public static Comparator<StructLike> forType(Types.StructType struct) {
    return new StructLikeComparator(struct);
  }

  public static <T> Comparator<List<T>> forType(Types.ListType list) {
    return new ListComparator<>(list);
  }

  @SuppressWarnings("unchecked")
  public static <T> Comparator<T> forType(Type.PrimitiveType type) {
    Comparator<?> cmp = COMPARATORS.get(type);
    if (cmp != null) {
      return (Comparator<T>) cmp;
    } else if (type instanceof Types.FixedType) {
      return (Comparator<T>) Comparators.unsignedBytes();
    } else if (type instanceof Types.DecimalType) {
      return (Comparator<T>) Comparator.naturalOrder();
    }

    throw new UnsupportedOperationException("Cannot determine comparator for type: " + type);
  }

  @SuppressWarnings("unchecked")
  private static <T> Comparator<T> internal(Type type) {
    if (type.isPrimitiveType()) {
      return forType(type.asPrimitiveType());
    } else if (type.isStructType()) {
      return (Comparator<T>) forType(type.asStructType());
    } else if (type.isListType()) {
      return (Comparator<T>) forType(type.asListType());
    }

    throw new UnsupportedOperationException("Cannot determine comparator for type: " + type);
  }

  private static class StructLikeComparator implements Comparator<StructLike> {
    private final Comparator<Object>[] comparators;
    private final Class<?>[] classes;

    private StructLikeComparator(Types.StructType struct) {
      this.comparators =
          struct.fields().stream()
              .map(
                  field ->
                      field.isOptional()
                          ? Comparators.nullsFirst().thenComparing(internal(field.type()))
                          : internal(field.type()))
              .toArray((IntFunction<Comparator<Object>[]>) Comparator[]::new);
      this.classes =
          struct.fields().stream()
              .map(field -> field.type().typeId().javaClass())
              .toArray(Class<?>[]::new);
    }

    @Override
    public int compare(StructLike o1, StructLike o2) {
      if (o1 == o2) {
        return 0;
      }

      for (int i = 0; i < comparators.length; i += 1) {
        Class<?> valueClass = classes[i];
        int cmp = comparators[i].compare(o1.get(i, valueClass), o2.get(i, valueClass));
        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }
  }

  private static class VariantComparator implements Comparator<Variant> {
    private static final VariantComparator INSTANCE = new VariantComparator();

    private VariantComparator() {}

    @Override
    public int compare(Variant o1, Variant o2) {
      VariantValue value1 = o1.value();
      VariantValue value2 = o2.value();
      // Compare based on the type of the VariantValue
      if (value1.type() != value2.type()) {
        return value1.type().compareTo(value2.type());
      }
      return compareElements(value1, value2);
    }

    private static int compareElements(Object elem1, Object elem2) {
      if (elem1 instanceof VariantArray && elem2 instanceof VariantArray) {
        return compareVariantArrays((VariantArray) elem1, (VariantArray) elem2);
      } else if (elem1 instanceof VariantObject && elem2 instanceof VariantObject) {
        return compareVariantObjects((VariantObject) elem1, (VariantObject) elem2);
      } else {
        VariantValue value1 = (VariantValue) elem1;
        VariantValue value2 = (VariantValue) elem2;
        switch (value1.type()) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            return forType(Types.IntegerType.get())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case FLOAT:
            return forType(Types.FloatType.get())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case DOUBLE:
            return forType(Types.DoubleType.get())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            BigDecimal decimalType = ((BigDecimal) value1.asPrimitive().get());
            int precision = decimalType.precision();
            int scale = decimalType.scale();
            return forType(Types.DecimalType.of(precision, scale))
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case DATE:
            return forType(Types.DateType.get())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case TIMESTAMPTZ:
            return forType(Types.TimestampType.withoutZone())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case TIMESTAMPNTZ:
            return forType(Types.TimestampNanoType.withoutZone())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case BINARY:
            return forType(Types.BinaryType.get())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          case STRING:
            return forType(Types.StringType.get())
                .compare(value1.asPrimitive().get(), value2.asPrimitive().get());
          default:
            throw new UnsupportedOperationException("Unsupported Variant type for comparison");
        }
      }
    }

    private static int compareVariantObjects(VariantObject obj1, VariantObject obj2) {
      List<String> keys1 = Lists.newArrayList(obj1.fieldNames());
      List<String> keys2 = Lists.newArrayList(obj2.fieldNames());

      if (keys1.isEmpty() || keys2.isEmpty()) {
        return (keys1.isEmpty() && keys2.isEmpty()) ? 0 : (keys1.isEmpty() ? -1 : 1);
      }

      if (keys1.size() > keys2.size()) {
        return 1;
      } else if (keys1.size() < keys2.size()) {
        return -1;
      }

      for (int i = 0; i < keys1.size(); i++) {
        String key1 = keys1.get(i);
        String key2 = keys2.get(i);
        int keyComparison = key1.compareTo(key2);
        if (keyComparison != 0) {
          return keyComparison;
        }
        int valueComparison = compareElements(obj1.get(key1), obj2.get(key2));
        if (valueComparison != 0) {
          return valueComparison;
        }
      }
      return 0;
    }

    public static int compareVariantArrays(VariantArray array1, VariantArray array2) {
      int len1 = array1.numElements();
      int len2 = array2.numElements();

      if (len1 > len2) {
        return 1;
      } else if (len1 < len2) {
        return -1;
      }

      for (int i = 0; i < len1; i++) {
        Object elem1 = array1.get(i);
        Object elem2 = array2.get(i);
        int comparison = compareElements(elem1, elem2);
        if (comparison != 0) {
          return comparison;
        }
      }

      return 0;
    }
  }

  private static class ListComparator<T> implements Comparator<List<T>> {
    private final Comparator<T> elementComparator;

    private ListComparator(Types.ListType list) {
      Comparator<T> elemComparator = internal(list.elementType());
      this.elementComparator =
          list.isElementOptional()
              ? Comparators.<T>nullsFirst().thenComparing(elemComparator)
              : elemComparator;
    }

    @Override
    public int compare(List<T> o1, List<T> o2) {
      if (o1 == o2) {
        return 0;
      }

      int length = Math.min(o1.size(), o2.size());
      for (int i = 0; i < length; i += 1) {
        int cmp = elementComparator.compare(o1.get(i), o2.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }

      return Integer.compare(o1.size(), o2.size());
    }
  }

  public static Comparator<ByteBuffer> unsignedBytes() {
    return UnsignedByteBufComparator.INSTANCE;
  }

  public static Comparator<byte[]> unsignedByteArrays() {
    return UnsignedByteArrayComparator.INSTANCE;
  }

  public static Comparator<ByteBuffer> signedBytes() {
    return Comparator.naturalOrder();
  }

  @SuppressWarnings("unchecked")
  public static <T> Comparator<T> nullsFirst() {
    return (Comparator<T>) NullsFirst.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public static <T> Comparator<T> nullsLast() {
    return (Comparator<T>) NullsLast.INSTANCE;
  }

  public static Comparator<CharSequence> charSequences() {
    return CharSeqComparator.INSTANCE;
  }

  public static Comparator<Variant> variantComparator() {
    return VariantComparator.INSTANCE;
  }

  public static Comparator<CharSequence> filePath() {
    return FilePathComparator.INSTANCE;
  }

  private static class NullsFirst<T> implements Comparator<T> {
    private static final NullsFirst<?> INSTANCE = new NullsFirst<>();

    private NullsFirst() {}

    @Override
    public int compare(T o1, T o2) {
      if (o1 == o2) {
        return 0;
      }

      if (o1 != null) {
        if (o2 != null) {
          return 0;
        }
        return 1;
      }

      return -1;
    }

    @Override
    public Comparator<T> thenComparing(Comparator<? super T> other) {
      return new NullSafeChainedComparator<>(this, other);
    }
  }

  private static class NullsLast<T> implements Comparator<T> {
    private static final NullsLast<?> INSTANCE = new NullsLast<>();

    private NullsLast() {}

    @Override
    public int compare(T o1, T o2) {
      if (o1 == o2) {
        return 0;
      }

      if (o1 != null) {
        if (o2 != null) {
          return 0;
        }
        return -1;
      }

      return 1;
    }

    @Override
    public Comparator<T> thenComparing(Comparator<? super T> other) {
      return new NullSafeChainedComparator<>(this, other);
    }
  }

  private static class NullSafeChainedComparator<T> implements Comparator<T> {
    private final Comparator<T> first;
    private final Comparator<? super T> second;

    NullSafeChainedComparator(Comparator<T> first, Comparator<? super T> second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public int compare(T o1, T o2) {
      if (o1 == o2) {
        return 0;
      }

      int cmp = first.compare(o1, o2);
      if (cmp == 0 && o1 != null) {
        return second.compare(o1, o2);
      }
      return cmp;
    }
  }

  private static class UnsignedByteBufComparator implements Comparator<ByteBuffer> {
    private static final UnsignedByteBufComparator INSTANCE = new UnsignedByteBufComparator();

    private UnsignedByteBufComparator() {}

    @Override
    public int compare(ByteBuffer buf1, ByteBuffer buf2) {
      if (buf1 == buf2) {
        return 0;
      }

      int len = Math.min(buf1.remaining(), buf2.remaining());

      // find the first difference and return
      int b1pos = buf1.position();
      int b2pos = buf2.position();
      for (int i = 0; i < len; i += 1) {
        // Conversion to int is what Byte.toUnsignedInt would do
        int cmp =
            Integer.compare(((int) buf1.get(b1pos + i)) & 0xff, ((int) buf2.get(b2pos + i)) & 0xff);
        if (cmp != 0) {
          return cmp;
        }
      }

      // if there are no differences, then the shorter seq is smaller
      return Integer.compare(buf1.remaining(), buf2.remaining());
    }
  }

  private static class UnsignedByteArrayComparator implements Comparator<byte[]> {
    private static final UnsignedByteArrayComparator INSTANCE = new UnsignedByteArrayComparator();

    private UnsignedByteArrayComparator() {}

    @Override
    public int compare(byte[] array1, byte[] array2) {
      if (array1 == array2) {
        return 0;
      }

      int len = Math.min(array1.length, array2.length);

      // find the first difference and return
      for (int i = 0; i < len; i += 1) {
        // Conversion to int is what Byte.toUnsignedInt would do
        int cmp = Integer.compare(((int) array1[i]) & 0xff, ((int) array2[i]) & 0xff);
        if (cmp != 0) {
          return cmp;
        }
      }

      // if there are no differences, then the shorter seq is smaller
      return Integer.compare(array1.length, array2.length);
    }
  }

  private static class CharSeqComparator implements Comparator<CharSequence> {
    private static final CharSeqComparator INSTANCE = new CharSeqComparator();

    private CharSeqComparator() {}

    /**
     * Java character supports only upto 3 byte UTF-8 characters. 4 byte UTF-8 character is
     * represented using two Java characters (using UTF-16 surrogate pairs). Character by character
     * comparison may yield incorrect results while comparing a 4 byte UTF-8 character to a java
     * char. Character by character comparison works as expected if both characters are <= 3 byte
     * UTF-8 character or both characters are 4 byte UTF-8 characters. isCharHighSurrogate method
     * detects a high surrogate (4-byte character) and considers that character to be
     * lexicographically greater than any 3 byte or lower UTF-8 character.
     */
    @Override
    public int compare(CharSequence s1, CharSequence s2) {
      if (s1 == s2) {
        return 0;
      }

      int len = Math.min(s1.length(), s2.length());

      // find the first difference and return
      for (int i = 0; i < len; i += 1) {
        char c1 = s1.charAt(i);
        char c2 = s2.charAt(i);
        boolean isC1HighSurrogate = UnicodeUtil.isCharHighSurrogate(c1);
        boolean isC2HighSurrogate = UnicodeUtil.isCharHighSurrogate(c2);
        if (isC1HighSurrogate && !isC2HighSurrogate) {
          return 1;
        }
        if (!isC1HighSurrogate && isC2HighSurrogate) {
          return -1;
        }
        int cmp = Character.compare(c1, c2);
        if (cmp != 0) {
          return cmp;
        }
      }

      // if there are no differences, then the shorter seq is first
      return Integer.compare(s1.length(), s2.length());
    }
  }

  private static class FilePathComparator implements Comparator<CharSequence> {
    private static final FilePathComparator INSTANCE = new FilePathComparator();

    private FilePathComparator() {}

    @Override
    public int compare(CharSequence s1, CharSequence s2) {
      if (s1 == s2) {
        return 0;
      }
      int count = s1.length();

      int cmp = Integer.compare(count, s2.length());
      if (cmp != 0) {
        return cmp;
      }

      if (s1 instanceof String && s2 instanceof String) {
        cmp = Integer.compare(s1.hashCode(), s2.hashCode());
        if (cmp != 0) {
          return cmp;
        }
      }
      // File paths inside a delete file normally have more identical chars at the beginning. For
      // example, a typical
      // path is like "s3:/bucket/db/table/data/partition/00000-0-[uuid]-00001.parquet".
      // The uuid is where the difference starts. So it's faster to find the first diff backward.
      for (int i = count - 1; i >= 0; i--) {
        cmp = Character.compare(s1.charAt(i), s2.charAt(i));
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
  }
}
