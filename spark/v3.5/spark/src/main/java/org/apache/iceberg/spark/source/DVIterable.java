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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

class DVIterable extends CloseableGroup implements CloseableIterable<InternalRow> {
  private final Puffin.ReadBuilder builder;
  private final PartitionSpec spec;
  private final DeleteFile deleteFile;
  private final Schema projection;

  DVIterable(InputFile inputFile, DeleteFile deleteFile, PartitionSpec spec, Schema projection) {
    this.deleteFile = deleteFile;
    this.builder = Puffin.read(inputFile);
    this.spec = spec;
    this.projection = projection;
  }

  @Override
  public CloseableIterator<InternalRow> iterator() {
    PuffinReader reader = builder.build();
    addCloseable(reader);
    return new DVIterator(reader);
  }

  private class DVIterator implements CloseableIterator<InternalRow> {
    private final PuffinReader reader;
    private Iterator<Long> positions = Collections.emptyIterator();
    private List<Object> rowValues;
    private Integer deletedPositionIndex;

    DVIterator(PuffinReader reader) {
      this.reader = reader;
      try {
        reader.fileMetadata().blobs().stream()
            .filter(
                blob ->
                    // read the correct blob for the referenced data file
                    Objects.equals(
                        deleteFile.referencedDataFile(),
                        blob.properties().get("referenced-data-file")))
            .findFirst()
            .ifPresent(
                blob -> {
                  // there should only be a single element
                  Pair<BlobMetadata, ByteBuffer> current =
                      Iterables.getOnlyElement(reader.readAll(ImmutableList.of(blob)));
                  List<Long> pos = Lists.newArrayList();
                  PositionDeleteIndex.deserialize(current.second().array(), deleteFile)
                      .forEach(pos::add);
                  this.positions = pos.iterator();
                });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return positions.hasNext();
    }

    @Override
    public InternalRow next() {
      long position = positions.next();

      if (null == rowValues) {
        this.rowValues = Lists.newArrayList();
        if (null != projection.findField(MetadataColumns.DELETE_FILE_PATH.fieldId())) {
          rowValues.add(UTF8String.fromString(deleteFile.referencedDataFile()));
        }

        if (null != projection.findField(MetadataColumns.DELETE_FILE_POS.fieldId())) {
          rowValues.add(position);
          // remember the index where the deleted position needs to be set
          deletedPositionIndex = rowValues.size() - 1;
        }

        if (null != projection.findField(MetadataColumns.DELETE_FILE_ROW_FIELD_ID)) {
          // there's no info about deleted rows with DVs, so always return null
          rowValues.add(null);
        }

        if (null != projection.findField(MetadataColumns.PARTITION_COLUMN_ID)) {
          StructInternalRow partition = new StructInternalRow(spec.partitionType());
          partition.setStruct(deleteFile.partition());
          rowValues.add(partition);
        }

        if (null != projection.findField(MetadataColumns.SPEC_ID_COLUMN_ID)) {
          rowValues.add(deleteFile.specId());
        }

        if (null != projection.findField(MetadataColumns.FILE_PATH_COLUMN_ID)) {
          rowValues.add(UTF8String.fromString(deleteFile.location()));
        }
      } else if (null != deletedPositionIndex) {
        // only update the deleted position if necessary, everything else stays the same
        rowValues.set(deletedPositionIndex, position);
      }

      return new GenericInternalRow(rowValues.toArray());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }

    @Override
    public void close() throws IOException {
      if (null != reader) {
        reader.close();
      }
    }
  }
}
