package org.apache.iceberg.parquet;

import java.lang.reflect.Array;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.types.Types;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnarBatchReader implements BatchedReader{
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

    public final void setPageSource(PageReadStore pageStore) {
        for (int i = 0; i < readers.length; i += 1) {
            readers[i].setPageSource(pageStore);
        }
    }

    public final ColumnarBatch read(ColumnarBatch ignore) {

        IcebergArrowColumnVector[] icebergArrowColumnVectors = (IcebergArrowColumnVector[]) Array.newInstance(IcebergArrowColumnVector.class,
            readers.length);

        int numRows = 0;
        for (int i = 0; i < readers.length; i += 1) {
            NullabilityVector nullabilityVector = new NullabilityVector(readers[i].batchSize());
            FieldVector vec = readers[i].read(nullabilityVector);
            icebergArrowColumnVectors[i] = new IcebergArrowColumnVector(new ArrowColumnVector(vec), nullabilityVector);
            //TODO: samarth reenable this check
            // if (i > 0) {
            //     //TODO: samarth time spent in this string builder!!
            //     Preconditions.checkState(numRows == vec.getValueCount(),
            //         "Different number of values returned by readers for columns: " +
            //             readers[i - 1] + " and " + readers[i]);
            // }
            numRows = vec.getValueCount();
        }

        ColumnarBatch batch = new ColumnarBatch(icebergArrowColumnVectors);
        batch.setNumRows(numRows);

        return batch;
    }

}

