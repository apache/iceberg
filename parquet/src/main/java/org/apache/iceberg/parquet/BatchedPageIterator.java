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
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.vector.*;
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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.types.VarcharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.types.Decimal;

import static java.lang.String.format;
import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

public class BatchedPageIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BatchedPageIterator.class);
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
    private ValuesReader values = null;


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

    public void setDictionary(Dictionary dict) {
        this.dict = dict;
    }

    public void reset() {
        this.page = null;
        this.triplesCount = 0;
        this.triplesRead = 0;
        this.definitionLevels = null;
        this.repetitionLevels = null;
        this.values = null;
        this.hasNext = false;
    }

    public int currentPageCount() {
        return triplesCount;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public int nextBatch(FieldVector vector, int numValsToRead) {
        final int valsToRead = Math.min(numValsToRead, triplesCount - triplesRead);
        int valsRead = 0;
        int ordinal = vector.getValueCount();
        int numVals = 0;
        int numNulls = 0;
        while (valsRead < valsToRead) {
            numVals = 0;
            while (valsRead < valsToRead && definitionLevels.nextInt() == desc.getMaxDefinitionLevel()) {
                numVals++;
                valsRead++;
            }

            PrimitiveType primitive = desc.getPrimitiveType();

            if (primitive.getOriginalType() != null) {
                switch (desc.getPrimitiveType().getOriginalType()) {
                    case ENUM:
                    case JSON:
                    case UTF8:
                        //valsRead = nextBatchedString(vector);
                        // Read next valsToRead values and put them in vector
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((VarCharVector) vector).setSafe(ordinal, values.readBytes().getBytesUnsafe());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT_8:
                    case INT_16:
                    case INT_32:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((IntVector) vector).setSafe(ordinal, values.readInteger());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }

                        break;
                    case DATE:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((DateDayVector) vector).setSafe(ordinal, values.readInteger());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT_64:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((BigIntVector) vector).setSafe(ordinal, values.readLong());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case TIMESTAMP_MICROS:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((TimeStampMicroTZVector) vector).setSafe(ordinal, values.readLong());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case TIMESTAMP_MILLIS:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((BigIntVector) vector).setSafe(ordinal, 1000 * values.readLong());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case DECIMAL:
                        DecimalMetadata decimal = primitive.getDecimalMetadata();
                        switch (primitive.getPrimitiveTypeName()) {
                            case BINARY:
                            case FIXED_LEN_BYTE_ARRAY:
                                for (int i = 0; i < numVals; i++) {
                                    try {
                                        /*Binary binaryValue = values.readBytes();

                                        Decimal decimalValue = Decimal.fromDecimal(new BigDecimal(new BigInteger(binaryValue.getBytes()), decimal.getScale()));
                                        ((DecimalVector) vector).setSafe(ordinal, decimalValue.toJavaBigDecimal());*/
                                        ((DecimalVector) vector).setBigEndian(ordinal, values.readBytes().getBytesUnsafe());
                                        ordinal++;
                                    } catch (RuntimeException e) {
                                        throw handleRuntimeException(e);
                                    }
                                }

                                break;
                            case INT64:
                                for (int i = 0; i < numVals; i++) {
                                    try {
                    /*long decimalLongValue = values.readLong();
                    Decimal decimalValue = Decimal.apply(decimalLongValue, precision, scale);

                    ((DecimalVector) vector).setSafe(ordinal, decimalValue.toJavaBigDecimal()); anjali??*/
                                        ((DecimalVector) vector).setBigEndian(ordinal, values.readBytes().getBytesUnsafe());
                                        ordinal++;
                                    } catch (RuntimeException e) {
                                        throw handleRuntimeException(e);
                                    }
                                }
                                break;
                            case INT32:
                                for (int i = 0; i < numVals; i++) {
                                    try {
                   /* int decimalIntValue = values.readInteger();
                    Decimal decimalValue = Decimal.apply(decimalIntValue, precision, scale);

                    ((DecimalVector) vector).setSafe(ordinal, decimalValue.toJavaBigDecimal()); anjali??*/
                                        ((DecimalVector) vector).setBigEndian(ordinal, values.readBytes().getBytesUnsafe());
                                        ordinal++;
                                    } catch (RuntimeException e) {
                                        throw handleRuntimeException(e);
                                    }
                                }

                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
                        }
                        break;
                    case BSON:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((VarCharVector) vector).setSafe(ordinal, values.readBytes().getBytesUnsafe());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported logical type: " + primitive.getOriginalType());
                }
            } else {
                switch (primitive.getPrimitiveTypeName()) {
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((VarCharVector) vector).setSafe(ordinal, values.readBytes().getBytesUnsafe());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT32:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((IntVector) vector).setSafe(ordinal, values.readInteger());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case FLOAT:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((Float4Vector) vector).setSafe(ordinal, values.readFloat());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    // if (expected != null && expected.typeId() == org.apache.iceberg.types.Type.TypeID.DOUBLE) {
                    //   return new ParquetValueReaders.FloatAsDoubleReader(desc);
                    // } else {
                    //   return new ParquetValueReaders.UnboxedReader<>(desc);
                    // }
                    case BOOLEAN:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((BitVector) vector).setSafe(ordinal, values.readBoolean() ? 1 : 0);
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT64:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((BigIntVector) vector).setSafe(ordinal, values.readLong());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case DOUBLE:
                        for (int i = 0; i < numVals; i++) {
                            try {
                                ((Float8Vector) vector).setSafe(ordinal, values.readDouble());
                                ordinal++;
                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + primitive);
                }
            }


            numNulls = 0;
            // Definition level found a null, read contiguous nulls
            while (valsRead < valsToRead && definitionLevels.nextInt() < desc.getMaxDefinitionLevel()) {
                numNulls++;
                valsRead++;
            }


            if (primitive.getOriginalType() != null) {
                switch (desc.getPrimitiveType().getOriginalType()) {
                    case ENUM:
                    case JSON:
                    case UTF8:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((VarCharVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT_8:
                    case INT_16:
                    case INT_32:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((IntVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case DATE:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((DateDayVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT_64:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((BigIntVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case TIMESTAMP_MICROS:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((TimeStampMicroTZVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case TIMESTAMP_MILLIS:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((BigIntVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case DECIMAL:
                        DecimalMetadata decimal = primitive.getDecimalMetadata();
                        switch (primitive.getPrimitiveTypeName()) {
                            case BINARY:
                            case FIXED_LEN_BYTE_ARRAY:
                                for (int i = 0; i < numNulls; i++) {
                                    try {
                                        ((DecimalVector) vector).setNull(ordinal);
                                        ordinal++;

                                    } catch (RuntimeException e) {
                                        throw handleRuntimeException(e);
                                    }
                                }
                                break;
                            case INT64:
                                for (int i = 0; i < numNulls; i++) {
                                    try {
                                        ((DecimalVector) vector).setNull(ordinal);
                                        ordinal++;

                                    } catch (RuntimeException e) {
                                        throw handleRuntimeException(e);
                                    }
                                }
                                break;
                            case INT32:
                                for (int i = 0; i < numNulls; i++) {
                                    try {
                                        ((DecimalVector) vector).setNull(ordinal);
                                        ordinal++;

                                    } catch (RuntimeException e) {
                                        throw handleRuntimeException(e);
                                    }
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
                        }
                        break;
                    case BSON:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((VarCharVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported logical type: " + primitive.getOriginalType());
                }
            } else {
                switch (primitive.getPrimitiveTypeName()) {
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((VarCharVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT32:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((IntVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case FLOAT:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((Float4Vector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    // if (expected != null && expected.typeId() == org.apache.iceberg.types.Type.TypeID.DOUBLE) {
                    //   return new ParquetValueReaders.FloatAsDoubleReader(desc);
                    // } else {
                    //   return new ParquetValueReaders.UnboxedReader<>(desc);
                    // }
                    case BOOLEAN:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((BitVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case INT64:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((BigIntVector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    case DOUBLE:
                        for (int i = 0; i < numNulls; i++) {
                            try {
                                ((Float8Vector) vector).setNull(ordinal);
                                ordinal++;

                            } catch (RuntimeException e) {
                                throw handleRuntimeException(e);
                            }
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + primitive);
                }
            }
        }
        vector.setValueCount(vector.getValueCount() + valsRead);
        triplesRead += valsRead;
        advance();
        return valsRead;
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
        ValuesReader previousReader = values;

        this.valueEncoding = dataEncoding;

        // TODO: May want to change this so that this class is not dictionary-aware.
        // For dictionary columns, this class could rely on wrappers to correctly handle dictionaries
        // This isn't currently possible because RLE must be read by getDictionaryBasedValuesReader
        if (dataEncoding.usesDictionary()) {
            if (dict == null) {
                throw new ParquetDecodingException(
                        "could not read page in col " + desc + " as the dictionary was missing for encoding " + dataEncoding);
            }
            this.values = dataEncoding.getDictionaryBasedValuesReader(desc, VALUES, dict);
        } else {
            this.values = dataEncoding.getValuesReader(desc, VALUES);
        }

//    if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
//      bindToDictionary(dictionary);
//    } else {
//      bind(path.getType());
//    }

        try {
            values.initFromPage(valueCount, in);
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page in col " + desc, e);
        }

        if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
                previousReader != null && previousReader instanceof RequiresPreviousReader) {
            // previous reader can only be set if reading sequentially
            ((RequiresPreviousReader) values).setPreviousReader(previousReader);
        }
    }


    private void initFromPage(DataPageV1 page) {
        this.triplesCount = page.getValueCount();
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(desc, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(desc, DEFINITION_LEVEL);
        this.repetitionLevels = new ValuesReaderIntIterator(rlReader);
        this.definitionLevels = new ValuesReaderIntIterator(dlReader);
        try {
            BytesInput bytes = page.getBytes();
            LOG.debug("page size {} bytes and {} records", bytes.size(), triplesCount);
            LOG.debug("reading repetition levels at 0");
            ByteBufferInputStream in = bytes.toInputStream();
            rlReader.initFromPage(triplesCount, in);
            LOG.debug("reading definition levels at {}", in.position());
            dlReader.initFromPage(triplesCount, in);
            LOG.debug("reading data at {}", in.position());
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
