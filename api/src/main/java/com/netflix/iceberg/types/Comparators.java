/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Comparators {
  public static Comparator<ByteBuffer> unsignedBytes() {
    return UnsignedByteBufComparator.INSTANCE;
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

  private static class NullsFirst<T> implements Comparator<T> {
    private static final NullsFirst<?> INSTANCE = new NullsFirst<>();

    private NullsFirst() {
    }

    @Override
    public int compare(T o1, T o2) {
      if (o1 != null) {
        if (o2 != null) {
          return 0;
        }
        return 1;
      } else if (o2 != null) {
        return -1;
      }
      return 0;
    }

    @Override
    public Comparator<T> thenComparing(Comparator<? super T> other) {
      return new NullSafeChainedComparator<>(this, other);
    }
  }

  private static class NullsLast<T> implements Comparator<T> {
    private static final NullsLast<?> INSTANCE = new NullsLast<>();

    private NullsLast() {
    }

    @Override
    public int compare(T o1, T o2) {
      if (o1 != null) {
        if (o2 != null) {
          return 0;
        }
        return -1;
      } else if (o2 != null) {
        return 1;
      }
      return 0;
    }

    @Override
    public Comparator<T> thenComparing(Comparator<? super T> other) {
      return new NullSafeChainedComparator<>(this, other);
    }
  }

  private static class NullSafeChainedComparator<T> implements Comparator<T> {
    private final Comparator<T> first;
    private final Comparator<? super T> second;

    public NullSafeChainedComparator(Comparator<T> first, Comparator<? super T> second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public int compare(T o1, T o2) {
      int cmp = first.compare(o1, o2);
      if (cmp == 0 && o1 != null) {
        return second.compare(o1, o2);
      }
      return cmp;
    }
  }

  private static class UnsignedByteBufComparator implements Comparator<ByteBuffer> {
    private static final UnsignedByteBufComparator INSTANCE = new UnsignedByteBufComparator();

    private UnsignedByteBufComparator() {
    }

    @Override
    public int compare(ByteBuffer buf1, ByteBuffer buf2) {
      int len = Math.min(buf1.remaining(), buf2.remaining());

      // find the first difference and return
      int b1pos = buf1.position();
      int b2pos = buf2.position();
      for (int i = 0; i < len; i += 1) {
        // Conversion to int is what Byte.toUnsignedInt would do
        int cmp = Integer.compare(
            ((int) buf1.get(b1pos + i)) & 0xff,
            ((int) buf2.get(b2pos + i)) & 0xff);
        if (cmp != 0) {
          return cmp;
        }
      }

      // if there are no differences, then the shorter seq is first
      return Integer.compare(buf1.remaining(), buf2.remaining());
    }
  }

  private static class CharSeqComparator implements Comparator<CharSequence> {
    private static final CharSeqComparator INSTANCE = new CharSeqComparator();

    private CharSeqComparator() {
    }

    @Override
    public int compare(CharSequence s1, CharSequence s2) {
      int len = Math.min(s1.length(), s2.length());

      // find the first difference and return
      for (int i = 0; i < len; i += 1) {
        int cmp = Character.compare(s1.charAt(i), s2.charAt(i));
        if (cmp != 0) {
          return cmp;
        }
      }

      // if there are no differences, then the shorter seq is first
      return Integer.compare(s1.length(), s2.length());
    }
  }
}
