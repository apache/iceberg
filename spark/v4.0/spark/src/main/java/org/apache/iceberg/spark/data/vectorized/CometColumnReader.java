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
package org.apache.iceberg.spark.data.vectorized;

import java.io.IOException;
import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.ColumnReader;
import org.apache.comet.parquet.TypeUtil;
import org.apache.comet.parquet.Utils;
import org.apache.comet.shaded.arrow.c.CometSchemaImporter;
import org.apache.comet.shaded.arrow.memory.RootAllocator;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnVector;

class CometColumnReader implements VectorizedReader<ColumnVector> {
  // use the Comet default batch size
  public static final int DEFAULT_BATCH_SIZE = 8192;

  private final ColumnDescriptor descriptor;
  private final DataType sparkType;

  // The delegated ColumnReader from Comet side
  private AbstractColumnReader delegate;
  private boolean initialized = false;
  private int batchSize = DEFAULT_BATCH_SIZE;
  private CometSchemaImporter importer;

  CometColumnReader(DataType sparkType, ColumnDescriptor descriptor) {
    this.sparkType = sparkType;
    this.descriptor = descriptor;
  }

  CometColumnReader(Types.NestedField field) {
    DataType dataType = SparkSchemaUtil.convert(field.type());
    StructField structField = new StructField(field.name(), dataType, false, Metadata.empty());
    this.sparkType = dataType;
    this.descriptor = TypeUtil.convertToParquet(structField);
  }

  public AbstractColumnReader delegate() {
    return delegate;
  }

  void setDelegate(AbstractColumnReader delegate) {
    this.delegate = delegate;
  }

  void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  public int batchSize() {
    return batchSize;
  }

  /**
   * This method is to initialized/reset the CometColumnReader. This needs to be called for each row
   * group after readNextRowGroup, so a new dictionary encoding can be set for each of the new row
   * groups.
   */
  public void reset() {
    if (importer != null) {
      importer.close();
    }

    if (delegate != null) {
      delegate.close();
    }

    this.importer = new CometSchemaImporter(new RootAllocator());
    this.delegate = Utils.getColumnReader(sparkType, descriptor, importer, batchSize, false, false);
    this.initialized = true;
  }

  public ColumnDescriptor descriptor() {
    return descriptor;
  }

  /** Returns the Spark data type for this column. */
  public DataType sparkType() {
    return sparkType;
  }

  /**
   * Set the page reader to be 'pageReader'.
   *
   * <p>NOTE: this should be called before reading a new Parquet column chunk, and after {@link
   * CometColumnReader#reset} is called.
   */
  public void setPageReader(PageReader pageReader) throws IOException {
    Preconditions.checkState(initialized, "Invalid state: 'reset' should be called first");
    ((ColumnReader) delegate).setPageReader(pageReader);
  }

  @Override
  public void close() {
    // close resources on native side
    if (importer != null) {
      importer.close();
    }

    if (delegate != null) {
      delegate.close();
    }
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  @Override
  public ColumnVector read(ColumnVector reuse, int numRowsToRead) {
    throw new UnsupportedOperationException("Not supported");
  }
}
