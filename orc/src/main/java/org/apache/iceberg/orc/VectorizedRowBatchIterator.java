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
package org.apache.iceberg.orc;

import java.io.IOException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.Pair;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * An adaptor so that the ORC RecordReader can be used as an Iterator. Because the same
 * VectorizedRowBatch is reused on each call to next, it gets changed when hasNext or next is
 * called.
 */
public class VectorizedRowBatchIterator
    implements CloseableIterator<Pair<VectorizedRowBatch, Long>> {
  private final String fileLocation;
  private final RecordReader rows;
  private final VectorizedRowBatch batch;
  private boolean advanced = false;
  private long batchOffsetInFile = 0;

  VectorizedRowBatchIterator(
      String fileLocation, TypeDescription schema, RecordReader rows, int recordsPerBatch) {
    this.fileLocation = fileLocation;
    this.rows = rows;
    this.batch = schema.createRowBatch(recordsPerBatch);
  }

  @Override
  public void close() throws IOException {
    rows.close();
  }

  private void advance() {
    if (!advanced) {
      try {
        batchOffsetInFile = rows.getRowNumber();
        rows.nextBatch(batch);
      } catch (IOException ioe) {
        throw new RuntimeIOException(ioe, "Problem reading ORC file %s", fileLocation);
      }
      advanced = true;
    }
  }

  @Override
  public boolean hasNext() {
    advance();
    return batch.size > 0;
  }

  @Override
  public Pair<VectorizedRowBatch, Long> next() {
    // make sure we have the next batch
    advance();
    // mark it as used
    advanced = false;
    return Pair.of(batch, batchOffsetInFile);
  }
}
