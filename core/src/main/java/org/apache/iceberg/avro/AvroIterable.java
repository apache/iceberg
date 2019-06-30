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

package org.apache.iceberg.avro;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;

public class AvroIterable<D> implements CloseableIterable<D> {

  private static final Logger logger = LoggerFactory.getLogger(AvroIterable.class);

  private enum State {
    NEW,
    OPEN,
    CLOSED
  }

  private final InputFile file;
  private final DatumReader<D> reader;
  private final Long start;
  private final Long end;
  private final boolean reuseContainers;
  private Map<String, String> metadata = null;
  private DataFileReader<D> fileReader = null;
  private State state = State.NEW;
  private final StackTraceElement[] createStack = Thread.currentThread().getStackTrace();

  AvroIterable(InputFile file, DatumReader<D> reader,
               Long start, Long length, boolean reuseContainers) {
    this.file = file;
    this.reader = reader;
    this.start = start;
    this.end = start != null ? start + length : null;
    this.reuseContainers = reuseContainers;
  }

  public Map<String, String> getMetadata() {
    checkState(state != State.CLOSED,  "%s is closed", this);
    if (metadata == null) {
      if (fileReader == null) {
        fileReader = newFileReader();
      }
      List<String> keys = fileReader.getMetaKeys();
      metadata = new HashMap<>(keys.size());
      for (String key : keys) {
        metadata.put(key, fileReader.getMetaString(key));
      }
    }
    return metadata;
  }

  @Override
  public Iterator<D> iterator() {
    checkState(state == State.NEW, "%s is already consumed or closed", this);
    state = State.OPEN;
    if (fileReader == null) {
      fileReader = newFileReader();
    }
    return reuseContainers ? new AvroReuseIterator() : fileReader;
  }

  @Override
  public void close() throws IOException {
    state = State.CLOSED;
    if (fileReader != null) {
      fileReader.close();
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + '@' + System.identityHashCode(this) +
        ", file " + file.toString();
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (state != State.CLOSED) {
      close();
      logger.warn("Unclosed {} created by:\n\t{}", this, new Object() {
        @Override
        public String toString() {
          return Joiner.on("\n\t").join(
              Arrays.copyOfRange(createStack, 2, createStack.length));
        }
      });
    }
  }

  private DataFileReader<D> newFileReader() {
    try {
      SeekableInput stream = AvroIO.stream(file.newStream(), file.getLength());
      return (start != null) ? new RangeDataFileReader(stream, reader, start, end) : new DataFileReader(stream, reader);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to open file: %s", file);
    }
  }

  private static class RangeDataFileReader<D> extends DataFileReader<D> {
    private final long end;

    RangeDataFileReader(SeekableInput sin, DatumReader<D> reader, long start, long end) throws IOException {
      super(sin, reader);
      this.end = end;
      sync(start);
    }

    @Override
    public boolean hasNext() {
      try {
        return super.hasNext() && !pastSync(end);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to check range end: %d", end);
      }
    }
  }

  private class AvroReuseIterator implements Iterator<D> {
    private D reused = null;

    @Override
    public boolean hasNext() {
      return fileReader.hasNext();
    }

    @Override
    public D next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      try {
        reused = fileReader.next(reused);
        return reused;
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to read next record");
      }
    }
  }
}
