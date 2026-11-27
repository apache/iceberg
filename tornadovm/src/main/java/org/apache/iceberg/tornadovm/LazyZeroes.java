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
package org.apache.iceberg.tornadovm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

/**
 * Zeroed host memory paid for when touched, not when allocated: anonymous {@code mmap} pages read
 * as zero and are backed only once written. A device region's buffers are sized to whole batches of
 * every column a shape could use, and {@code Arena.allocate} zero-fills all of it up front --
 * gigabytes a region set, hundreds of milliseconds -- most of which a query never touches. Linux
 * only, and needs {@code --enable-native-access=ALL-UNNAMED} (Java 21 refuses the downcalls
 * otherwise); elsewhere, or without it, the caller allocates as before. Memory mapped this way is
 * not counted against {@code -XX:MaxDirectMemorySize}, only what is touched is resident.
 */
final class LazyZeroes {
  /** Buffers from this size; smaller ones are cheap to zero. */
  static final long THRESHOLD = 1L << 20;

  private static final int PROT_READ_WRITE = 0x1 | 0x2;
  private static final int MAP_PRIVATE_ANONYMOUS = 0x02 | 0x20;
  private static final MethodHandle MMAP;
  private static final MethodHandle MUNMAP;
  static final boolean AVAILABLE;

  static {
    MethodHandle mmap = null;
    MethodHandle munmap = null;
    if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux")
        && !Boolean.getBoolean("iceberg.tornadovm.eagerZeroes")) {
      try {
        Linker linker = Linker.nativeLinker();
        mmap =
            linker.downcallHandle(
                linker.defaultLookup().find("mmap").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_LONG));
        munmap =
            linker.downcallHandle(
                linker.defaultLookup().find("munmap").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
      } catch (RuntimeException e) {
        mmap = null;
        munmap = null;
      }
    }
    MMAP = mmap;
    MUNMAP = munmap;
    AVAILABLE = mmap != null;
  }

  private LazyZeroes() {}

  /** {@code size} zeroed bytes, unmapped when {@code arena} closes. */
  static MemorySegment allocate(long size, Arena arena) {
    MemorySegment address;
    try {
      address =
          (MemorySegment)
              MMAP.invokeExact(
                  MemorySegment.NULL, size, PROT_READ_WRITE, MAP_PRIVATE_ANONYMOUS, -1, 0L);
    } catch (Throwable e) {
      throw new IllegalStateException("mmap of " + size + " bytes failed", e);
    }
    if (address.address() == -1L) {
      throw new IllegalStateException("mmap of " + size + " bytes failed");
    }
    return address.reinterpret(size, arena, LazyZeroes::unmap);
  }

  private static void unmap(MemorySegment segment) {
    try {
      int ignored = (int) MUNMAP.invokeExact(segment, segment.byteSize());
    } catch (Throwable e) {
      throw new IllegalStateException("munmap failed", e);
    }
  }
}
