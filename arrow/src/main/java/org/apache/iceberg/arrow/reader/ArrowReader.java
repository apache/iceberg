package org.apache.iceberg.arrow.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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

public class ArrowReader {

  public static InternalRowOverArrowBatchIterator fromBatchIterator(
      Iterator<ArrowRecordBatch> arrowBatchIter,
      StructType sparkSchema,
      String timeZoneId) {

    // StructType sparkSchema = SparkSchemaUtil.convert(icebergSchema);

    Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, timeZoneId);
    BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("fromBatchIterator", 0, Long.MAX_VALUE);

    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);

    return new InternalRowOverArrowBatchIterator(arrowBatchIter, allocator, root);
  }

  public static class InternalRowOverArrowBatchIterator implements Iterator<InternalRow>, Closeable {

    private Iterator<ArrowRecordBatch> arrowBatchIter;
    private Iterator<InternalRow> rowIter;
    private BufferAllocator allocator;
    private VectorSchemaRoot root;

    InternalRowOverArrowBatchIterator(Iterator<ArrowRecordBatch> arrowBatchIter,
        BufferAllocator allocator,
        VectorSchemaRoot root) {

      this.arrowBatchIter = arrowBatchIter;
      this.allocator = allocator;
      this.root = root;

      // if (arrowBatchIter.hasNext()) {
      //   rowIter = nextBatch();
      // } else {
      //   rowIter = Collections.emptyIterator();
      // }
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
        root.close();
        allocator.close();
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

    Iterator<InternalRow> rowIterator;
    VectorSchemaRoot root;
    BufferAllocator allocator;
    int maxRecordsPerBatch;
    ArrowWriter arrowWriter;
    VectorUnloader unloader;

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

        root.close();
        allocator.close();
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
