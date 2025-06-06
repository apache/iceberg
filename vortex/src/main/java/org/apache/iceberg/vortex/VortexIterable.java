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
package org.apache.iceberg.vortex;

import dev.vortex.api.Array;
import dev.vortex.api.ArrayIterator;
import dev.vortex.api.DType;
import dev.vortex.api.File;
import dev.vortex.api.Files;
import dev.vortex.api.ScanOptions;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VortexIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(VortexIterable.class);

  private final InputFile inputFile;
  private final Optional<Expression> filterPredicate;
  private final long[] rowRange;
  private final Function<DType, VortexRowReader<T>> rowReaderFunc;
  private final Function<DType, VortexBatchReader<T>> batchReaderFunction;
  private final List<String> projection;

  VortexIterable(
      InputFile inputFile,
      Schema icebergSchema,
      Optional<Expression> filterPredicate,
      long[] rowRange,
      Function<DType, VortexRowReader<T>> readerFunction,
      Function<DType, VortexBatchReader<T>> batchReaderFunction) {
    this.inputFile = inputFile;
    // We have the file schema, we need to assign Iceberg IDs to the entire file schema
    this.projection = Lists.transform(icebergSchema.columns(), Types.NestedField::name);
    this.filterPredicate = filterPredicate;
    this.rowRange = rowRange;
    this.rowReaderFunc = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
  }

  @Override
  public CloseableIterator<T> iterator() {
    File vortexFile = newVortexFile(inputFile);
    addCloseable(vortexFile);

    // Return the filtered scan, and then the projection, etc.
    Optional<dev.vortex.api.Expression> scanPredicate =
        filterPredicate.map(
            icebergExpression -> {
              Schema fileSchema = VortexSchemas.convert(vortexFile.getDType());
              return ConvertFilterToVortex.convert(fileSchema, icebergExpression);
            });

    Optional<long[]> rowRange = Optional.ofNullable(this.rowRange);

    ArrayIterator batchStream =
        vortexFile.newScan(
            ScanOptions.builder()
                .addAllColumns(projection)
                .predicate(scanPredicate)
                .rowIndices(rowRange)
                .build());
    Preconditions.checkNotNull(batchStream, "batchStream");

    DType dtype = batchStream.getDataType();
    CloseableIterator<Array> batchIterator = CloseableIterator.withClose(batchStream);

    if (rowReaderFunc != null) {
      VortexRowReader<T> rowFunction = rowReaderFunc.apply(dtype);
      return new VortexRowIterator<>(batchIterator, rowFunction);
    } else {
      VortexBatchReader<T> batchTransform = batchReaderFunction.apply(dtype);
      return CloseableIterator.transform(batchIterator, batchTransform::read);
    }
  }

  private static File newVortexFile(InputFile inputFile) {
    Preconditions.checkArgument(
        inputFile instanceof HadoopInputFile, "Vortex only supports HadoopInputFile currently");
    LOG.debug("opening Vortex file: {}", inputFile);

    HadoopInputFile hadoopInputFile = (HadoopInputFile) inputFile;
    URI path = hadoopInputFile.getPath().toUri();
    switch (path.getScheme()) {
      case "s3a":
        return Files.open(path, s3PropertiesFromHadoopConf(hadoopInputFile.getConf()));
      case "wasb":
      case "wasbs":
      case "abfs":
      case "abfss":
        return Files.open(path, azurePropertiesFromHadoopConf(hadoopInputFile.getConf()));
      case "file":
        return Files.open(path, Map.of());
      default:
        // TODO(aduffy): add support for Azure
        throw new IllegalArgumentException("Unsupported scheme: " + path.getScheme());
    }
  }

  static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
  static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
  static final String FS_S3A_SESSION_TOKEN = "fs.s3a.session.token";
  static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
  static final String FS_S3A_ENDPOINT_REGION = "fs.s3a.endpoint.region";

  private static Map<String, String> s3PropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexS3Properties properties = new VortexS3Properties();

    for (Map.Entry<String, String> entry : hadoopConf) {
      switch (entry.getKey()) {
        case FS_S3A_ACCESS_KEY:
          properties.setAccessKeyId(entry.getValue());
          break;
        case FS_S3A_SECRET_KEY:
          properties.setSecretAccessKey(entry.getValue());
          break;
        case FS_S3A_SESSION_TOKEN:
          properties.setSessionToken(entry.getValue());
          break;
        case FS_S3A_ENDPOINT:
          String qualified = entry.getValue();
          if (!qualified.startsWith("http")) {
            qualified = "https://" + qualified;
          }
          properties.setEndpoint(qualified);
          break;
        case FS_S3A_ENDPOINT_REGION:
          properties.setRegion(entry.getValue());
          break;
        default:
          LOG.trace(
              "Ignoring unknown s3a connector property: {}={}", entry.getKey(), entry.getValue());
          break;
      }
    }

    return properties.asProperties();
  }

  static final String ACCESS_KEY_PREFIX = "fs.azure.account.key";
  static final String FIXED_TOKEN_PREFIX = "fs.azure.sas.fixed.token.";

  private static Map<String, String> azurePropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexAzureProperties properties = new VortexAzureProperties();

    // TODO(aduffy): match on storage account name.
    for (Map.Entry<String, String> entry : hadoopConf) {
      String configKey = entry.getKey();
      if (configKey.startsWith(ACCESS_KEY_PREFIX)) {
        properties.setAccessKey(entry.getValue());
      } else if (configKey.startsWith(FIXED_TOKEN_PREFIX)) {
        properties.setSasKey(entry.getValue());
      } else {
        LOG.trace("Ignoring unknown azure connector property: {}={}", configKey, entry.getValue());
      }
    }

    return properties.asProperties();
  }

  static class VortexRowIterator<T> extends CloseableGroup implements CloseableIterator<T> {
    private final CloseableIterator<Array> stream;
    private final VortexRowReader<T> rowReader;

    private Array currentBatch = null;
    private int batchIndex = 0;
    private int batchLen = 0;

    VortexRowIterator(CloseableIterator<Array> stream, VortexRowReader<T> rowReader) {
      this.stream = stream;
      addCloseable(stream);
      this.rowReader = rowReader;
      if (stream.hasNext()) {
        currentBatch = stream.next();
        batchLen = (int) currentBatch.getLen();
      }
    }

    @Override
    public void close() throws IOException {
      // Do not close the ArrayStream, it is closed by the parent.
      currentBatch.close();
      currentBatch = null;
    }

    @Override
    public boolean hasNext() {
      // See if we need to fill a new batch first.
      if (currentBatch == null || batchIndex == batchLen) {
        advance();
      }

      return currentBatch != null;
    }

    @Override
    public T next() {
      T nextRow = rowReader.read(currentBatch, batchIndex);
      batchIndex++;
      return nextRow;
    }

    private void advance() {
      if (stream.hasNext()) {
        currentBatch = stream.next();
        batchIndex = 0;
        batchLen = (int) currentBatch.getLen();
      } else {
        currentBatch = null;
        batchLen = 0;
      }
    }
  }
}
