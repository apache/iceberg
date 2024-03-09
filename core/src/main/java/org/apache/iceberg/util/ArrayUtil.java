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
package org.apache.iceberg.util;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.iceberg.relocated.com.google.common.primitives.Longs;

public class ArrayUtil {
  private ArrayUtil() {}

  public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public static final short[] EMPTY_SHORT_ARRAY = new short[0];
  public static final int[] EMPTY_INT_ARRAY = new int[0];
  public static final long[] EMPTY_LONG_ARRAY = new long[0];
  public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
  public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

  public static List<Integer> toIntList(int[] ints) {
    if (ints != null) {
      return IntStream.of(ints).boxed().collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static int[] toIntArray(List<Integer> ints) {
    if (ints != null) {
      return ints.stream().mapToInt(v -> v).toArray();
    } else {
      return null;
    }
  }

  public static List<Long> toLongList(long[] longs) {
    if (longs != null) {
      return LongStream.of(longs).boxed().collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static List<Long> toUnmodifiableLongList(long[] longs) {
    if (longs != null) {
      return Collections.unmodifiableList(Longs.asList(longs));
    } else {
      return null;
    }
  }

  public static long[] toLongArray(List<Long> longs) {
    if (longs != null) {
      return longs.stream().mapToLong(v -> v).toArray();
    } else {
      return null;
    }
  }

  /**
   * Converts an array of object Booleans to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Boolean} array, may be {@code null}
   * @return a {@code boolean} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static boolean[] toPrimitive(final Boolean[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_BOOLEAN_ARRAY;
    }
    final boolean[] result = new boolean[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].booleanValue();
    }
    return result;
  }

  /**
   * Converts an array of object Bytes to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Byte} array, may be {@code null}
   * @return a {@code byte} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static byte[] toPrimitive(final Byte[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_BYTE_ARRAY;
    }
    final byte[] result = new byte[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].byteValue();
    }
    return result;
  }

  /**
   * Converts an array of object Shorts to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Short} array, may be {@code null}
   * @return a {@code byte} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static short[] toPrimitive(final Short[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_SHORT_ARRAY;
    }
    final short[] result = new short[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].shortValue();
    }
    return result;
  }

  /**
   * Converts an array of object Integers to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Integer} array, may be {@code null}
   * @return an {@code int} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static int[] toPrimitive(final Integer[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_INT_ARRAY;
    }
    final int[] result = new int[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].intValue();
    }
    return result;
  }

  /**
   * Converts an array of object Longs to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Long} array, may be {@code null}
   * @return a {@code long} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static long[] toPrimitive(final Long[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_LONG_ARRAY;
    }
    final long[] result = new long[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].longValue();
    }
    return result;
  }

  /**
   * Converts an array of object Floats to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Float} array, may be {@code null}
   * @return a {@code float} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static float[] toPrimitive(final Float[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_FLOAT_ARRAY;
    }
    final float[] result = new float[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].floatValue();
    }
    return result;
  }

  /**
   * Converts an array of object Doubles to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array a {@code Double} array, may be {@code null}
   * @return a {@code double} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static double[] toPrimitive(final Double[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_DOUBLE_ARRAY;
    }
    final double[] result = new double[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].doubleValue();
    }
    return result;
  }

  /**
   * Copies the given array and adds the given element at the end of the new array.
   *
   * <p>The new array contains the same elements of the input array plus the given element in the
   * last position. The component type of the new array is the same as that of the input array.
   *
   * <p>If the input array is {@code null}, a new one element array is returned whose component type
   * is the same as the element, unless the element itself is null, in which case the return type is
   * Object[]
   *
   * <pre>
   * ArrayUtils.add(null, null)      = IllegalArgumentException
   * ArrayUtils.add(null, "a")       = ["a"]
   * ArrayUtils.add(["a"], null)     = ["a", null]
   * ArrayUtils.add(["a"], "b")      = ["a", "b"]
   * ArrayUtils.add(["a", "b"], "c") = ["a", "b", "c"]
   * </pre>
   *
   * This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param <T> the component type of the array
   * @param array the array to "add" the element to, may be {@code null}
   * @param element the object to add, may be {@code null}
   * @return A new array containing the existing elements plus the new element The returned array
   *     type will be that of the input array (unless null), in which case it will have the same
   *     type as the element. If both are null, an IllegalArgumentException is thrown
   * @since 2.1
   * @throws IllegalArgumentException if both arguments are null
   */
  public static <T> T[] add(final T[] array, final T element) {
    Class<?> type;
    if (array != null) {
      type = array.getClass().getComponentType();
    } else if (element != null) {
      type = element.getClass();
    } else {
      throw new IllegalArgumentException("Arguments cannot both be null");
    }
    @SuppressWarnings("unchecked") // type must be T
    final T[] newArray = (T[]) copyArrayGrow1(array, type);
    newArray[newArray.length - 1] = element;
    return newArray;
  }

  /**
   * Returns a copy of the given array of size 1 greater than the argument. The last value of the
   * array is left to the default value.
   *
   * <p>This code is borrowed from `org.apache.commons:commons-lang3`.
   *
   * @param array The array to copy, must not be {@code null}.
   * @param newArrayComponentType If {@code array} is {@code null}, create a size 1 array of this
   *     type.
   * @return A new copy of the array of size 1 greater than the input.
   */
  private static Object copyArrayGrow1(final Object array, final Class<?> newArrayComponentType) {
    if (array != null) {
      final int arrayLength = Array.getLength(array);
      final Object newArray =
          Array.newInstance(array.getClass().getComponentType(), arrayLength + 1);
      System.arraycopy(array, 0, newArray, 0, arrayLength);
      return newArray;
    }
    return Array.newInstance(newArrayComponentType, 1);
  }

  public static boolean isStrictlyAscending(long[] array) {
    for (int index = 1; index < array.length; index++) {
      if (array[index] <= array[index - 1]) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] concat(Class<T> type, T[]... arrays) {
    T[] result = (T[]) Array.newInstance(type, totalLength(arrays));

    int currentLength = 0;

    for (T[] array : arrays) {
      int length = array.length;
      if (length > 0) {
        System.arraycopy(array, 0, result, currentLength, length);
        currentLength += length;
      }
    }

    return result;
  }

  private static int totalLength(Object[][] arrays) {
    int totalLength = 0;

    for (Object[] array : arrays) {
      totalLength += array.length;
    }

    return totalLength;
  }
}
