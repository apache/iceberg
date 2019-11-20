package org.apache.iceberg.parquet.vectorized;

import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.parquet.org.apache.iceberg.parquet.arrow.IcebergArrowColumnVector;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's
 * vectorized read path. The {@link ColumnarBatch} returned is created by passing in the
 * Arrow vectors populated via delegated read calls to {@linkplain VectorizedArrowReader VectorReader(s)}.
 */
public class ColumnarBatchReaders implements VectorizedReader {
    private final VectorizedArrowReader[] readers;

    public ColumnarBatchReaders(List<Type> types,
                                Types.StructType icebergExpectedFields,
                                List<VectorizedReader> readers) {
        this.readers = (VectorizedArrowReader[]) Array.newInstance(
                VectorizedArrowReader.class, readers.size());
        int i = 0;
        for (VectorizedReader reader : readers) {
            this.readers[i] = (VectorizedArrowReader) reader;
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
            VectorHolder holder = readers[i].read(nullabilityHolder);
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

