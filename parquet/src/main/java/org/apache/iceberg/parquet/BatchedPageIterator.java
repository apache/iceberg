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

package org.apache.iceberg.parquet;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

public class BatchedPageIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BatchedPageIterator.class);
    private VectorizedValuesReader vectorizedValuesReader;
    private final int batchSize;

    public BatchedPageIterator(ColumnDescriptor desc, String writerVersion, int batchSize) {
        this.desc = desc;
        this.writerVersion = writerVersion;
        this.batchSize = batchSize;
    }


    private final ColumnDescriptor desc;
    private final String writerVersion;

    // iterator state
    private boolean hasNext = false;
    private int triplesRead = 0;
    private int currentDL = 0;
    private int currentRL = 0;

    // page bookkeeping
    private Dictionary dict = null;
    private DataPage page = null;
    private int triplesCount = 0;
    private Encoding valueEncoding = null;
    private IntIterator definitionLevels = null;
    private IntIterator repetitionLevels = null;
    private BytesReader bytesReader = null;
    private ValuesReader valuesReader = null;

    public void setPage(DataPage page) {
        Preconditions.checkNotNull(page, "Cannot read from null page");
        this.page = page;
        this.page.accept(new DataPage.Visitor<ValuesReader>() {
            @Override
            public ValuesReader visit(DataPageV1 dataPageV1) {
                initFromPage(dataPageV1);
                return null;
            }

            @Override
            public ValuesReader visit(DataPageV2 dataPageV2) {
                initFromPage(dataPageV2);
                return null;
            }
        });
        this.triplesRead = 0;
        advance();
    }

    // Dictionary is set per row group
    public void setDictionary(Dictionary dict) {
        this.dict = dict;
    }

    public void reset() {
        this.page = null;
        this.triplesCount = 0;
        this.triplesRead = 0;
        this.definitionLevels = null;
        this.repetitionLevels = null;
        this.bytesReader = null;
        this.hasNext = false;
    }

    public int currentPageCount() {
        return triplesCount;
    }

    public boolean hasNext() {
        return hasNext;
    }

    /**
     * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
     * This method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow
     * vector. It appropriately sets the validity buffer in the Arrow vector.
     */
    public int nextBatchIntegers(final FieldVector vector, final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth, NullabilityHolder holder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfIntegers(vector, numValsInVector, typeWidth, actualBatchSize, holder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    // ok so if the dictionary is available here, what do we do?
    // the dicitonary needs to be set only once I think?
    // we use the dictionary and set it in the value vector?
    // we also know that all we need to do is read integers




    /**
     * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
     * This method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow
     * vector. It appropriately sets the validity buffer in the Arrow vector.
     */
    public int nextBatchLongs(final FieldVector vector, final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth, NullabilityHolder holder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfLongs(vector, numValsInVector, typeWidth, actualBatchSize, holder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    /**
     * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
     * This method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow
     * vector. It appropriately sets the validity buffer in the Arrow vector.
     */
    public int nextBatchFloats(final FieldVector vector, final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth, NullabilityHolder holder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfFloats(vector, numValsInVector, typeWidth, actualBatchSize, holder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    /**
     * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
     * This method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow
     * vector. It appropriately sets the validity buffer in the Arrow vector.
     */
    public int nextBatchDoubles(final FieldVector vector, final int expectedBatchSize,
        final int numValsInVector,
        final int typeWidth, NullabilityHolder holder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfDoubles(vector, numValsInVector, typeWidth, actualBatchSize, holder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    private int getActualBatchSize(int expectedBatchSize) {
        return Math.min(expectedBatchSize, triplesCount - triplesRead);
    }

    /**
     * Method for reading a batch of decimals backed by INT32 and INT64 parquet data types.
     * Arrow stores all decimals in 16 bytes. This method provides the necessary padding to the decimals read.
     */
    public int nextBatchIntLongBackedDecimal(final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
                                             final int typeWidth, NullabilityHolder nullabilityHolder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfIntLongBackedDecimals(vector, numValsInVector, typeWidth, actualBatchSize, nullabilityHolder,
            bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    /**
     * Method for reading a batch of decimals backed by fixed length byte array parquet data type.
     * Arrow stores all decimals in 16 bytes. This method provides the necessary padding to the decimals read.
     * Moreover, Arrow interprets the decimals in Arrow buffer as little endian. Parquet stores fixed length decimals
     * as big endian. So, this method uses {@link DecimalVector#setBigEndian(int, byte[])} method so that the data in
     * Arrow vector is indeed little endian.
     */
    public int nextBatchFixedLengthDecimal(final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
                                           final int typeWidth, NullabilityHolder nullabilityHolder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfFixedLengthDecimals(vector, numValsInVector, typeWidth, actualBatchSize, nullabilityHolder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    /**
     * Method for reading a batch of variable width data type (ENUM, JSON, UTF8, BSON).
     */
    public int nextBatchVarWidthType(final FieldVector vector, final int expectedBatchSize, final int numValsInVector
        , NullabilityHolder nullabilityHolder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchVarWidth(vector, numValsInVector, actualBatchSize, nullabilityHolder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    /**
     * Method for reading batches of fixed width binary type (e.g. BYTE[7]).
     * Spark does not support fixed width binary data type. To work around this limitation, the data is read as
     * fixed width binary from parquet and stored in a {@link VarBinaryVector} in Arrow.
     */
    public int nextBatchFixedWidthBinary(final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
                                         final int typeWidth, NullabilityHolder nullabilityHolder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfFixedWidthBinary(vector, numValsInVector, typeWidth, actualBatchSize, nullabilityHolder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    /**
     * Method for reading batches of booleans.
     */
    public int nextBatchBoolean(final FieldVector vector, final int expectedBatchSize, final int numValsInVector, NullabilityHolder nullabilityHolder) {
        final int actualBatchSize = getActualBatchSize(expectedBatchSize);
        if (actualBatchSize <= 0) {
            return 0;
        }
        vectorizedValuesReader.readBatchOfBooleans(vector, numValsInVector, actualBatchSize,
            nullabilityHolder, bytesReader);
        triplesRead += actualBatchSize;
        this.hasNext = triplesRead < triplesCount;
        return actualBatchSize;
    }

    private void advance() {
        if (triplesRead < triplesCount) {
            this.hasNext = true;
        } else {
            this.hasNext = false;
        }
    }

    RuntimeException handleRuntimeException(RuntimeException e) {
        if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, valueEncoding) &&
                e instanceof ArrayIndexOutOfBoundsException) {
            // this is probably PARQUET-246, which may happen if reading data with
            // MR because this can't be detected without reading all footers
            throw new ParquetDecodingException("Read failure possibly due to " +
                    "PARQUET-246: try setting parquet.split.files to false",
                    new ParquetDecodingException(
                            format("Can't read value in column %s at value %d out of %d in current page. " +
                                            "repetition level: %d, definition level: %d",
                                    desc, triplesRead, triplesCount, currentRL, currentDL),
                            e));
        }
        throw new ParquetDecodingException(
                format("Can't read value in column %s at value %d out of %d in current page. " +
                                "repetition level: %d, definition level: %d",
                        desc, triplesRead, triplesCount, currentRL, currentDL),
                e);
    }

    private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
        ValuesReader previousReader = bytesReader;

        this.valueEncoding = dataEncoding;

        // TODO: May want to change this so that this class is not dictionary-aware.
        // For dictionary columns, this class could rely on wrappers to correctly handle dictionaries
        // This isn't currently possible because RLE must be read by getDictionaryBasedValuesReader
        if (dataEncoding.usesDictionary() && dict == null) {
                throw new ParquetDecodingException(
                        "could not read page in col " + desc + " as the dictionary was missing for encoding " + dataEncoding);
        } else {
            //if (ParquetUtil.isVarWidthType(desc) || ParquetUtil.isBooleanType(desc)) {
            // if (ParquetUtil.isBooleanType(desc)) {
            //     this.valuesReader = dataEncoding.getValuesReader(desc, VALUES);
            //     try {
            //         valuesReader.initFromPage(valueCount, in);
            //     } catch (IOException e) {
            //         throw new ParquetDecodingException("could not read page in col " + desc, e);
            //     }
            // } else {
                this.bytesReader = new BytesReader();
                bytesReader.initFromPage(valueCount, in);
            //}
        }

        //    if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
        //      bindToDictionary(dictionary);
        //    } else {
        //      bind(path.getType());
        //    }
        if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
                previousReader != null && previousReader instanceof RequiresPreviousReader) {
            // previous reader can only be set if reading sequentially
            ((RequiresPreviousReader) bytesReader).setPreviousReader(previousReader);
        }
    }


    private void initFromPage(DataPageV1 page) {
        this.triplesCount = page.getValueCount();
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(desc, REPETITION_LEVEL);
        int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
        ValuesReader dlReader = this.vectorizedValuesReader = new VectorizedValuesReader(
            bitWidth, desc.getMaxDefinitionLevel());
        this.repetitionLevels = new ValuesReaderIntIterator(rlReader);
        this.definitionLevels = new ValuesReaderIntIterator(dlReader);
        try {
            BytesInput bytes = page.getBytes();
            ByteBufferInputStream in = bytes.toInputStream();
            rlReader.initFromPage(triplesCount, in);
            dlReader.initFromPage(triplesCount, in);
            initDataReader(page.getValueEncoding(), in, page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + desc, e);
        }
    }

    private void initFromPage(DataPageV2 page) {
        this.triplesCount = page.getValueCount();
        this.repetitionLevels = newRLEIterator(desc.getMaxRepetitionLevel(), page.getRepetitionLevels());
        this.definitionLevels = newRLEIterator(desc.getMaxDefinitionLevel(), page.getDefinitionLevels());
        LOG.debug("page data size {} bytes and {} records", page.getData().size(), triplesCount);
        try {
            initDataReader(page.getDataEncoding(), page.getData().toInputStream(), triplesCount);
            int bitWidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
            this.vectorizedValuesReader = new VectorizedValuesReader(bitWidth, false,
                    desc.getMaxDefinitionLevel());
            vectorizedValuesReader.initFromPage(triplesCount, page.getDefinitionLevels().toInputStream());

        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + desc, e);
        }
    }

    private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
        try {
            if (maxLevel == 0) {
                return new NullIntIterator();
            }
            return new RLEIntIterator(
                    new RunLengthBitPackingHybridDecoder(
                            BytesUtils.getWidthFromMaxInt(maxLevel),
                            bytes.toInputStream()));
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read levels in page for col " + desc, e);
        }
    }

    static abstract class IntIterator {
        abstract int nextInt();
    }

    static class ValuesReaderIntIterator extends IntIterator {
        ValuesReader delegate;

        ValuesReaderIntIterator(ValuesReader delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        int nextInt() {
            return delegate.readInteger();
        }
    }

    static class RLEIntIterator extends IntIterator {
        RunLengthBitPackingHybridDecoder delegate;

        RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
            this.delegate = delegate;
        }

        @Override
        int nextInt() {
            try {
                return delegate.readInt();
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }
    }

    private static final class NullIntIterator extends IntIterator {
        @Override
        int nextInt() {
            return 0;
        }
    }
}
