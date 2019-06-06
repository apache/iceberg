package org.apache.iceberg.arrow.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.TaskCompletionListener;

/***
 * This is a helper class for Arrow reading. It provides two main converter methods.
 * These converter methods are currently used to first convert a Parquet FileIterator
 * into Iterator over ArrowRecordBatches. Second, the ArrowRecordBatch is made
 * into Columnar Batch and exposed as an Iterator over InternalRow. The second step is to
 * done to conform to Spark's current interface. When Spark adds Arrow support we will
 * take the second iterator out and just return the first one.
 */
public class ArrowReader {

  /***
   * Accepts an iterator over ArrowRecordBatches and copies into ColumnarBatches.
   * Since Spark uses Iterator over InternalRow we return this over ColumarBatch.
   * @param arrowBatchIter
   * @param sparkSchema
   * @param timeZoneId
   * @return
   */
  public static InternalRowOverArrowBatchIterator fromBatchIterator(
      Iterator<ArrowRecordBatch> arrowBatchIter,
      StructType sparkSchema,
      String timeZoneId) {

    // timeZoneId required for TimestampType in StructType
    Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, timeZoneId);
    BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("fromBatchIterator", 0, Long.MAX_VALUE);

    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);

    return new InternalRowOverArrowBatchIterator(arrowBatchIter, allocator, root);
  }

  @NotThreadSafe
  public static class InternalRowOverArrowBatchIterator implements Iterator<InternalRow>, Closeable {

    private final Iterator<ArrowRecordBatch> arrowBatchIter;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;

    private Iterator<InternalRow> rowIter;

    InternalRowOverArrowBatchIterator(Iterator<ArrowRecordBatch> arrowBatchIter,
        BufferAllocator allocator,
        VectorSchemaRoot root) {

      this.arrowBatchIter = arrowBatchIter;
      this.allocator = allocator;
      this.root = root;

    }



    @Override
    public boolean hasNext() {
      if (rowIter != null && rowIter.hasNext()) {
        return true;
      }
      if (arrowBatchIter.hasNext()) {
        rowIter = nextBatch();
        return true;
      } else {
        try {
          close();
        } catch (IOException ioe) {
          throw new RuntimeException("Encountered an error while closing iterator. "+ioe.getMessage(), ioe);
        }
        return false;
      }
    }

    @Override
    public InternalRow next() {
      return rowIter.next();
    }

    private Iterator<InternalRow> nextBatch() {
      ArrowRecordBatch arrowRecordBatch = arrowBatchIter.next();
      VectorLoader vectorLoader = new VectorLoader(root);
      vectorLoader.load(arrowRecordBatch);
      arrowRecordBatch.close();

      List<FieldVector> fieldVectors = root.getFieldVectors();
      ColumnVector[] columns = new ColumnVector[fieldVectors.size()];
      for(int i=0; i<fieldVectors.size(); i++) {
        columns[i] = new ArrowColumnVector(fieldVectors.get(i));
      }

      ColumnarBatch batch = new ColumnarBatch(columns);
      batch.setNumRows(root.getRowCount());

      return batch.rowIterator();
    }


    @Override
    public void close() throws IOException {
      // arrowWriter.finish();
      root.close();
      allocator.close();
    }

  }

  /**
   * Acceepts Iterator over InternalRow coming in from ParqeutReader's FileIterator
   * and creates ArrowRecordBatches over that by collecting rows from the input iter.
   * Each next() call over this iterator will collect up to maxRecordsPerBatch rows
   * at a time and create an Arrow batch with it and returns an iterator over that.
   * @param rowIter
   * @param sparkSchema
   * @param maxRecordsPerBatch
   * @param timezonId
   * @return
   */
  public static ArrowRecordBatchIterator toBatchIterator(
      Iterator<InternalRow> rowIter,
      StructType sparkSchema, int maxRecordsPerBatch,
      String timezonId) {

    // StructType sparkSchema = SparkSchemaUtil.convert(icebergSchema);
    TaskContext context = TaskContext.get();

    Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, timezonId);
    BufferAllocator allocator = ArrowUtils.rootAllocator().newChildAllocator(
        "toBatchIterator",
        0,
        Long.MAX_VALUE);
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);

    context.addTaskCompletionListener(new TaskCompletionListener() {
      @Override
      public void onTaskCompletion(TaskContext context) {
        root.close();
        allocator.close();
      }
    });

    return new ArrowRecordBatchIterator(rowIter, root, allocator, maxRecordsPerBatch);
  }


  public static class ArrowRecordBatchIterator implements Iterator<ArrowRecordBatch>, Closeable {

    final Iterator<InternalRow> rowIterator;
    final VectorSchemaRoot root;
    final BufferAllocator allocator;
    final int maxRecordsPerBatch;
    final ArrowWriter arrowWriter;
    final VectorUnloader unloader;

    ArrowRecordBatchIterator(Iterator<InternalRow> rowIterator,
        VectorSchemaRoot root,
        BufferAllocator allocator,
        int maxRecordsPerBatch) {

      this.unloader = new VectorUnloader(root);
      this.arrowWriter = ArrowWriter.create(root);
      this.rowIterator = rowIterator;
      this.root = root;
      this.allocator = allocator;
      this.maxRecordsPerBatch = maxRecordsPerBatch;
    }

    @Override
    public boolean hasNext() {

      if (!rowIterator.hasNext()) {

        try {
          close();
        } catch (IOException ioe) {
          throw new RuntimeException("Encountered an error while closing iterator. "+ioe.getMessage(), ioe);
        }
        return false;
      }

      return true;
    }

    @Override
    public ArrowRecordBatch next() {

      int rowCount = 0;

      while (rowIterator.hasNext() && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
        InternalRow row = rowIterator.next();
        arrowWriter.write(row);
        rowCount += 1;
      }
      arrowWriter.finish();
      ArrowRecordBatch batch = unloader.getRecordBatch();
      return batch;
    }

    @Override
    public void close() throws IOException {
      // arrowWriter.finish();
      root.close();
      allocator.close();
    }
  }
}
