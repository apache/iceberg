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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Implementation of the Source V2 API which uses an iterator to read the elements, and uses a
 * single thread to do so.
 *
 * @param <T> The return type of the source
 */
public abstract class SingleThreadedIteratorSource<T>
    implements Source<
            T,
            SingleThreadedIteratorSource.GlobalSplit<T>,
            Collection<SingleThreadedIteratorSource.GlobalSplit<T>>>,
        ResultTypeQueryable<T> {
  private static final String PARALLELISM_ERROR = "Parallelism should be set to 1";

  /**
   * Creates the iterator to return the elements which then emitted by the source.
   *
   * @return iterator for the elements
   */
  abstract Iterator<T> createIterator();

  /**
   * Serializes the iterator, which is used to save and restore the state of the source.
   *
   * @return serializer for the iterator
   */
  abstract SimpleVersionedSerializer<Iterator<T>> getIteratorSerializer();

  @Override
  public SplitEnumerator<GlobalSplit<T>, Collection<GlobalSplit<T>>> createEnumerator(
      SplitEnumeratorContext<GlobalSplit<T>> enumContext) {
    Preconditions.checkArgument(enumContext.currentParallelism() == 1, PARALLELISM_ERROR);
    return new IteratorSourceEnumerator<>(
        enumContext, ImmutableList.of(new GlobalSplit<>(createIterator())));
  }

  @Override
  public SplitEnumerator<GlobalSplit<T>, Collection<GlobalSplit<T>>> restoreEnumerator(
      SplitEnumeratorContext<GlobalSplit<T>> enumContext, Collection<GlobalSplit<T>> checkpoint) {
    Preconditions.checkArgument(enumContext.currentParallelism() == 1, PARALLELISM_ERROR);
    return new IteratorSourceEnumerator<>(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<GlobalSplit<T>> getSplitSerializer() {
    return new SplitSerializer<>(getIteratorSerializer());
  }

  @Override
  public SimpleVersionedSerializer<Collection<GlobalSplit<T>>> getEnumeratorCheckpointSerializer() {
    return new CheckpointSerializer<>(getIteratorSerializer());
  }

  @Override
  public SourceReader<T, GlobalSplit<T>> createReader(SourceReaderContext readerContext)
      throws Exception {
    Preconditions.checkArgument(readerContext.getIndexOfSubtask() == 0, PARALLELISM_ERROR);
    return new IteratorSourceReader<>(readerContext);
  }

  /** The single split of the {@link SingleThreadedIteratorSource}. */
  static class GlobalSplit<T> implements IteratorSourceSplit<T, Iterator<T>> {
    private final Iterator<T> iterator;

    GlobalSplit(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public String splitId() {
      return "1";
    }

    @Override
    public Iterator<T> getIterator() {
      return iterator;
    }

    @Override
    public IteratorSourceSplit<T, Iterator<T>> getUpdatedSplitForIterator(
        final Iterator<T> newIterator) {
      return new GlobalSplit<>(newIterator);
    }

    @Override
    public String toString() {
      return String.format("GlobalSplit (%s)", iterator);
    }
  }

  private static final class SplitSerializer<T>
      implements SimpleVersionedSerializer<GlobalSplit<T>> {
    private final SimpleVersionedSerializer<Iterator<T>> serializer;

    SplitSerializer(SimpleVersionedSerializer<Iterator<T>> serializer) {
      this.serializer = serializer;
    }

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(GlobalSplit<T> split) throws IOException {
      return serializer.serialize(split.iterator);
    }

    @Override
    public GlobalSplit<T> deserialize(int version, byte[] serialized) throws IOException {
      return new GlobalSplit<>(serializer.deserialize(version, serialized));
    }
  }

  private static final class CheckpointSerializer<T>
      implements SimpleVersionedSerializer<Collection<GlobalSplit<T>>> {
    private static final int CURRENT_VERSION = 1;
    private final SimpleVersionedSerializer<Iterator<T>> iteratorSerializer;

    CheckpointSerializer(SimpleVersionedSerializer<Iterator<T>> iteratorSerializer) {
      this.iteratorSerializer = iteratorSerializer;
    }

    @Override
    public int getVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<GlobalSplit<T>> checkpoint) throws IOException {
      Preconditions.checkArgument(checkpoint.size() < 2, PARALLELISM_ERROR);
      if (checkpoint.isEmpty()) {
        return new byte[] {0};
      } else {
        byte[] iterator = iteratorSerializer.serialize(checkpoint.iterator().next().getIterator());
        byte[] result = new byte[iterator.length + 1];
        result[0] = 1;
        System.arraycopy(iterator, 0, result, 1, iterator.length);
        return result;
      }
    }

    @Override
    public Collection<GlobalSplit<T>> deserialize(int version, byte[] serialized)
        throws IOException {
      if (serialized[0] == 0) {
        return Lists.newArrayList();
      } else {
        byte[] iterator = new byte[serialized.length - 1];
        System.arraycopy(serialized, 1, iterator, 0, serialized.length - 1);
        return Lists.newArrayList(
            new GlobalSplit<>(iteratorSerializer.deserialize(version, iterator)));
      }
    }
  }
}
