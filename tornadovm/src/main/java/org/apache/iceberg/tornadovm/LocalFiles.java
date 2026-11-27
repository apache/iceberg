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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Local paths for the files a device scan reads and writes. cuDF opens files by path, and a table
 * on an object store (S3, GCS, ADLS, through Iceberg's {@link FileIO}) has none: its files are
 * downloaded once into a local cache -- Iceberg files never change once written, so a cached copy
 * never goes stale -- and files a rewrite writes go to a local staging file that is uploaded after.
 *
 * <p>The cache is {@code iceberg.tornadovm.cacheDir} (by default under {@code java.io.tmpdir}),
 * bounded to {@code iceberg.tornadovm.cacheBytes} (64 GB by default), least recently used files
 * evicted first.
 */
final class LocalFiles {
  private static final Path CACHE =
      Paths.get(
          System.getProperty(
              "iceberg.tornadovm.cacheDir",
              System.getProperty("java.io.tmpdir") + "/iceberg-tornadovm-cache"));
  private static final long LIMIT = Long.getLong("iceberg.tornadovm.cacheBytes", 64L << 30);
  private static final int FETCHERS = Integer.getInteger("iceberg.tornadovm.fetchThreads", 16);

  // Cached files by local path, least recently used first, with their sizes.
  private static final Map<Path, Long> CACHED = new LinkedHashMap<>(16, 0.75f, true);
  private static final Map<String, Object> FETCHING = Maps.newConcurrentMap();
  private static long cachedBytes = 0;

  private static final ExecutorService POOL =
      Executors.newFixedThreadPool(
          FETCHERS,
          runnable -> {
            Thread thread = new Thread(runnable, "tornadovm-fetch");
            thread.setDaemon(true);
            return thread;
          });

  private LocalFiles() {}

  /** Whether cuDF can open the location as it is. */
  static boolean isLocal(String location) {
    return location.startsWith("file:") || location.startsWith("/");
  }

  /** A local path for the location: itself if local, else its cached copy, fetched if need be. */
  static String path(String location, FileIO io) {
    return path(location, io, false);
  }

  /**
   * {@code whole}: the caller reads every column (a rewrite), so a partial copy is completed;
   * otherwise a partial copy serves too (footer reads, and scans whose prefetch fetched the columns
   * they read).
   */
  private static String path(String location, FileIO io, boolean whole) {
    if (location.startsWith("file:")) {
      return Paths.get(URI.create(location)).toString();
    }
    if (location.startsWith("/")) {
      return location;
    }
    Path local = CACHE.resolve(digest(location) + suffix(location));
    synchronized (FETCHING.computeIfAbsent(location, ignored -> new Object())) {
      synchronized (CACHED) {
        if (CACHED.containsKey(local)
            && Files.exists(local)
            && !(whole && PARTIAL.containsKey(local))) {
          return local.toString();
        }
      }
      try {
        Files.createDirectories(CACHE);
        Path partial = Files.createTempFile(CACHE, "fetch-", ".part");
        try (InputStream in = io.newInputFile(location).newStream()) {
          Files.copy(in, partial, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.move(
            partial, local, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        PARTIAL.remove(local);
        remember(local, Files.size(local));
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot fetch " + location, e);
      }
    }
    return local.toString();
  }

  // Cached copies that hold only some columns, by local path: the field ids fetched so far. A copy
  // not in this map holds the whole file.
  private static final Map<Path, Set<Integer>> PARTIAL = Maps.newConcurrentMap();

  /** How much of a file's end one read takes, to get its footer in a single request. */
  private static final int TAIL = 64 << 10;

  /** Ranges closer than this are fetched as one read: fewer requests for a few wasted bytes. */
  private static final long COALESCE =
      Long.getLong("iceberg.tornadovm.fetchCoalesceBytes", 1L << 20);

  /**
   * {@link #prefetch(Collection, FileIO)} for Parquet data files of which a scan reads only some
   * columns: each file's copy is a sparse file of its full length holding its footer, its magic and
   * the column chunks (with their page indexes) of the given field ids, each at its own offset --
   * what a reader of those columns touches -- so cuDF opens it as the original file. A copy that
   * already holds some columns gets the missing ones.
   */
  static void prefetch(Collection<String> locations, FileIO io, Set<Integer> fieldIds) {
    List<CompletableFuture<String>> fetches = Lists.newArrayList();
    for (String location : locations) {
      if (!isLocal(location)) {
        fetches.add(
            CompletableFuture.supplyAsync(
                () ->
                    location.endsWith(".parquet")
                        ? pathWithColumns(location, io, fieldIds)
                        : path(location, io, true),
                POOL));
      }
    }
    fetches.forEach(CompletableFuture::join);
  }

  private static String pathWithColumns(String location, FileIO io, Set<Integer> fieldIds) {
    Path local = CACHE.resolve(digest(location) + suffix(location));
    synchronized (FETCHING.computeIfAbsent(location, ignored -> new Object())) {
      Set<Integer> held;
      synchronized (CACHED) {
        boolean cached = CACHED.containsKey(local) && Files.exists(local);
        held = cached ? PARTIAL.get(local) : null;
        if (cached && (held == null || held.containsAll(fieldIds))) {
          return local.toString();
        }
      }
      try {
        InputFile remote = io.newInputFile(location);
        long length = remote.getLength();
        Files.createDirectories(CACHE);
        long fetched = 0;
        // One stream for the file; every read is a bounded range read (one ranged GET on S3).
        try (SeekableInputStream in = remote.newStream();
            FileChannel out =
                FileChannel.open(local, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
          if (held == null) {
            // A new copy: the magic and the footer, at the end of a sparse file of the file's
            // length. One read of the tail usually holds the whole footer.
            out.truncate(0);
            int tailLength = (int) Math.min(length, TAIL);
            byte[] tail = read(in, length - tailLength, tailLength);
            int footerLength =
                ByteBuffer.wrap(tail, tailLength - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            long footerStart = length - 8 - footerLength;
            if (footerStart < length - tailLength) {
              tail = read(in, footerStart, footerLength + 8);
              tailLength = footerLength + 8;
            }
            write(out, length - tailLength, tail);
            write(out, 0, read(in, 0, 4));
            fetched += tailLength + 4L;
            held = ConcurrentHashMap.newKeySet();
          }
          Set<Integer> missing = Sets.newHashSet(fieldIds);
          missing.removeAll(held);
          for (long[] range : coalesce(columnRanges(local, missing))) {
            write(out, range[0], read(in, range[0], (int) (range[1] - range[0])));
            fetched += range[1] - range[0];
          }
        }
        held.addAll(fieldIds);
        PARTIAL.put(local, held);
        remember(local, fetched + (CACHED.containsKey(local) ? CACHED.get(local) : 0));
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot fetch " + location, e);
      }
    }
    return local.toString();
  }

  /** The byte ranges of the given columns' chunks and page indexes, from the copy's footer. */
  private static List<long[]> columnRanges(Path local, Set<Integer> fieldIds) throws IOException {
    List<long[]> ranges = Lists.newArrayList();
    try (org.apache.parquet.hadoop.ParquetFileReader reader =
        org.apache.parquet.hadoop.ParquetFileReader.open(
            new org.apache.parquet.io.LocalInputFile(local))) {
      org.apache.parquet.schema.MessageType schema = reader.getFileMetaData().getSchema();
      for (org.apache.parquet.hadoop.metadata.BlockMetaData block :
          reader.getFooter().getBlocks()) {
        for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
          org.apache.parquet.schema.Type top = schema.getType(chunk.getPath().toArray()[0]);
          if (top.getId() == null || !fieldIds.contains(top.getId().intValue())) {
            continue;
          }
          long start = chunk.getStartingPos();
          ranges.add(new long[] {start, start + chunk.getTotalSize()});
          if (chunk.getColumnIndexReference() != null) {
            long offset = chunk.getColumnIndexReference().getOffset();
            ranges.add(new long[] {offset, offset + chunk.getColumnIndexReference().getLength()});
          }
          if (chunk.getOffsetIndexReference() != null) {
            long offset = chunk.getOffsetIndexReference().getOffset();
            ranges.add(new long[] {offset, offset + chunk.getOffsetIndexReference().getLength()});
          }
        }
      }
    }
    return ranges;
  }

  /** The ranges sorted, with those closer than {@link #COALESCE} merged. */
  private static List<long[]> coalesce(List<long[]> ranges) {
    ranges.sort(Comparator.comparingLong(range -> range[0]));
    List<long[]> merged = Lists.newArrayList();
    for (long[] range : ranges) {
      long[] last = merged.isEmpty() ? null : merged.get(merged.size() - 1);
      if (last != null && range[0] <= last[1] + COALESCE) {
        last[1] = Math.max(last[1], range[1]);
      } else {
        merged.add(range.clone());
      }
    }
    return merged;
  }

  /** Bytes of the stream at an offset: a ranged read where the stream offers one (S3, GCS). */
  private static byte[] read(SeekableInputStream in, long offset, int length) throws IOException {
    byte[] bytes = new byte[length];
    if (in instanceof RangeReadable ranged) {
      ranged.readFully(offset, bytes, 0, length);
      return bytes;
    }
    in.seek(offset);
    int done = 0;
    while (done < length) {
      int count = in.read(bytes, done, length - done);
      if (count < 0) {
        throw new EOFException("Short read at " + offset);
      }
      done += count;
    }
    return bytes;
  }

  private static void write(FileChannel out, long offset, byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long position = offset;
    while (buffer.hasRemaining()) {
      position += out.write(buffer, position);
    }
  }

  /** Fetches the remote ones among the locations at once, ahead of their first use. */
  static void prefetch(Collection<String> locations, FileIO io) {
    List<CompletableFuture<String>> fetches = Lists.newArrayList();
    for (String location : locations) {
      if (!isLocal(location)) {
        fetches.add(CompletableFuture.supplyAsync(() -> path(location, io, true), POOL));
      }
    }
    fetches.forEach(CompletableFuture::join);
  }

  /** A local file to write a data file into before it is uploaded to its location. */
  static Path stagingFile() {
    try {
      Files.createDirectories(CACHE);
      return Files.createTempFile(CACHE, "write-", ".parquet");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Uploads a staged file to its location, then deletes the local copy. */
  static void upload(Path staged, String location, FileIO io) {
    try (InputStream in = Files.newInputStream(staged);
        OutputStream out = io.newOutputFile(location).create()) {
      in.transferTo(out);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot upload " + location, e);
    }
    try {
      Files.deleteIfExists(staged);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Deletes a staged file nothing was written to. */
  static void discard(Path staged) {
    try {
      Files.deleteIfExists(staged);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Forgets and deletes every cached copy, so the next read fetches from the object store. */
  static void clear() {
    List<Path> cached;
    synchronized (CACHED) {
      cached = Lists.newArrayList(CACHED.keySet());
      CACHED.clear();
      PARTIAL.clear();
      cachedBytes = 0;
    }
    for (Path path : cached) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static void remember(Path local, long bytes) {
    List<Path> evicted = Lists.newArrayList();
    synchronized (CACHED) {
      Long previous = CACHED.put(local, bytes);
      cachedBytes += bytes - (previous == null ? 0 : previous);
      Iterator<Map.Entry<Path, Long>> eldest = CACHED.entrySet().iterator();
      while (cachedBytes > LIMIT && eldest.hasNext()) {
        Map.Entry<Path, Long> entry = eldest.next();
        if (entry.getKey().equals(local)) {
          continue;
        }
        cachedBytes -= entry.getValue();
        evicted.add(entry.getKey());
        PARTIAL.remove(entry.getKey());
        eldest.remove();
      }
    }
    for (Path path : evicted) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        // A file a reader still has open is deleted when it closes it; nothing to do here.
      }
    }
  }

  private static String suffix(String location) {
    int dot = location.lastIndexOf('.');
    return dot > location.lastIndexOf('/') ? location.substring(dot) : "";
  }

  private static String digest(String location) {
    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256").digest(location.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash, 0, 16);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
