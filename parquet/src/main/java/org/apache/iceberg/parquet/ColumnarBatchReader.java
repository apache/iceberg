package org.apache.iceberg.parquet;

import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.parquet.org.apache.iceberg.parquet.arrow.IcebergArrowColumnVector;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

public class ColumnarBatchReader implements BatchedReader {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnarBatchReader.class);
    private final VectorReader[] readers;

    public ColumnarBatchReader(List<Type> types,
                               Types.StructType icebergExpectedFields,
                               List<BatchedReader> readers) {
        this.readers = (VectorReader[]) Array.newInstance(
                VectorReader.class, readers.size());
        int i = 0;
        for (BatchedReader reader : readers) {
            this.readers[i] = (VectorReader) reader;
            i++;
        }

    }

    public final void setRowGroupInfo(PageReadStore pageStore,
                                      DictionaryPageReadStore dictionaryPageReadStore,
                                      Map<ColumnPath, Boolean> columnDictEncoded) {
        for (int i = 0; i < readers.length; i += 1) {
            readers[i].setRowGroupInfo(pageStore, dictionaryPageReadStore, columnDictEncoded);
        }
    }

    public final ColumnarBatch read(ColumnarBatch ignore) {
        IcebergArrowColumnVector[] icebergArrowColumnVectors = (IcebergArrowColumnVector[]) Array.newInstance(IcebergArrowColumnVector.class,
            readers.length);
        int numRows = 0;
        for (int i = 0; i < readers.length; i += 1) {
            NullabilityHolder nullabilityHolder = new NullabilityHolder(readers[i].batchSize());
            VectorReader.VectorHolder holder = readers[i].read(nullabilityHolder);
            FieldVector vector = holder.getVector();
            icebergArrowColumnVectors[i] = new IcebergArrowColumnVector(holder, nullabilityHolder);
            if (i > 0 && numRows != vector.getValueCount()) {
                throw new IllegalStateException("Different number of values returned by readers" +
                    "for columns " + readers[i - 1] + " and " + readers[i]);
            }
            numRows = vector.getValueCount();
        }

        ColumnarBatch batch = new ColumnarBatch(icebergArrowColumnVectors);
        batch.setNumRows(numRows);

        return batch;
    }

}

