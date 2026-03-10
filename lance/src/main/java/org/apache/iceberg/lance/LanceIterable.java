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
package org.apache.iceberg.lance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * A {@link CloseableIterable} implementation for reading data from Lance format files.
 *
 * <p>In the current implementation, this reads the text-based format produced by {@link
 * LanceFileAppender}. In a production implementation, this would use the Lance JNI bridge to read
 * native Lance columnar data and convert it to GenericRecord via Arrow vectors.
 *
 * @param <D> the type of data records to read (typically GenericRecord)
 */
public class LanceIterable<D> implements CloseableIterable<D> {
  private final InputFile inputFile;
  private final Schema schema;
  private final Schema projectedSchema;

  /**
   * Create a new LanceIterable.
   *
   * @param inputFile the input file to read from
   * @param schema the full file schema
   * @param projectedSchema the projected (output) schema, may be a subset of the full schema
   */
  public LanceIterable(InputFile inputFile, Schema schema, Schema projectedSchema) {
    Preconditions.checkNotNull(inputFile, "Input file cannot be null");
    Preconditions.checkNotNull(schema, "Schema cannot be null");
    this.inputFile = inputFile;
    this.schema = schema;
    this.projectedSchema = projectedSchema != null ? projectedSchema : schema;
  }

  @Override
  public CloseableIterator<D> iterator() {
    try {
      List<D> records = readAll();
      return new ListCloseableIterator<>(records);
    } catch (IOException e) {
      throw new org.apache.iceberg.exceptions.RuntimeIOException(e, "Failed to read Lance file");
    }
  }

  @Override
  public void close() throws IOException {
    // No resources to close for text-based reader
  }

  /**
   * Read all records from the file.
   *
   * @return a list of records
   * @throws IOException if reading fails
   */
  @SuppressWarnings("unchecked")
  private List<D> readAll() throws IOException {
    List<D> records = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(inputFile.newStream(), StandardCharsets.UTF_8))) {
      // Read header
      String headerLine = reader.readLine();
      if (headerLine == null || !headerLine.startsWith("LANCE_V1")) {
        throw new IOException("Not a valid Lance file: missing LANCE_V1 header");
      }

      // Skip schema and record count lines
      String line;
      boolean dataSectionStarted = false;
      while ((line = reader.readLine()) != null) {
        if ("---".equals(line.trim())) {
          dataSectionStarted = true;
          continue;
        }
        if (!dataSectionStarted) {
          continue;
        }

        // Parse record from pipe-delimited values
        String[] values = line.split("\\|", -1);
        GenericRecord record = GenericRecord.create(projectedSchema);
        List<Types.NestedField> columns = schema.columns();
        List<Types.NestedField> projectedColumns = projectedSchema.columns();

        for (Types.NestedField projCol : projectedColumns) {
          // Find the index of this column in the full schema
          int fullIndex = -1;
          for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).fieldId() == projCol.fieldId()) {
              fullIndex = i;
              break;
            }
          }

          if (fullIndex >= 0 && fullIndex < values.length) {
            String rawValue = values[fullIndex];
            if ("null".equals(rawValue)) {
              record.setField(projCol.name(), null);
            } else {
              Object parsed = parseValue(rawValue, projCol.type());
              record.setField(projCol.name(), parsed);
            }
          }
        }

        records.add((D) record);
      }
    }

    return records;
  }

  /**
   * Parse a string value into the appropriate Java type.
   */
  private Object parseValue(String raw, org.apache.iceberg.types.Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return Boolean.parseBoolean(raw);
      case INTEGER:
        return Integer.parseInt(raw);
      case LONG:
        return Long.parseLong(raw);
      case FLOAT:
        return Float.parseFloat(raw);
      case DOUBLE:
        return Double.parseDouble(raw);
      case STRING:
        return raw;
      default:
        // For other types, return as string for now
        return raw;
    }
  }

  /**
   * A CloseableIterator backed by a List.
   */
  private static class ListCloseableIterator<E> implements CloseableIterator<E> {
    private final Iterator<E> delegate;

    ListCloseableIterator(List<E> list) {
      this.delegate = list.iterator();
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return delegate.next();
    }

    @Override
    public void close() throws IOException {
      // nothing to close for a list-backed iterator
    }
  }
}
