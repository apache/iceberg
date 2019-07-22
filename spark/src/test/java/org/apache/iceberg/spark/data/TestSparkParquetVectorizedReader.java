package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.vector.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;


public class TestSparkParquetVectorizedReader extends AvroDataTest {

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {

    // Write test data
    Assume.assumeTrue("Parquet Avro cannot write non-string map keys", null == TypeUtil.find(schema,
        type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    List<GenericData.Record> expected = RandomData.generateList(schema, 100, 0L);

    // write a test parquet file using iceberg writer
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      writer.addAll(expected);
    }


    try(CloseableIterable<ColumnarBatch> batchReader = Parquet.read(Files.localInput(testFile))
    .project(schema)
    .createReaderFunc(type -> VectorizedSparkParquetReaders.buildReader(schema, schema, type))
    .build()) {

      Iterator<ColumnarBatch> batches = batchReader.iterator();
      int numRowsRead = 0;
      while(batches.hasNext()) {

        ColumnarBatch batch = batches.next();
        numRowsRead += batch.numRows();
        System.out.println("Batch read with "+batch.numRows()+" rows. Read "+numRowsRead+" till now.");
      }

      Assert.assertEquals(expected.size(), numRowsRead);

    }
  }




  @Test
  public void testArray() throws IOException {
    System.out.println("Not Supported");
  }

  @Test
  public void testArrayOfStructs() throws IOException {
    System.out.println("Not Supported");
  }

  @Test
  public void testMap() throws IOException {
    System.out.println("Not Supported");
  }

  @Test
  public void testNumericMapKey() throws IOException {
    System.out.println("Not Supported");
  }

  @Test
  public void testComplexMapKey() throws IOException {
    System.out.println("Not Supported");
  }

  @Test
  public void testMapOfStructs() throws IOException {
    System.out.println("Not Supported");
  }

  @Test
  public void testMixedTypes() throws IOException {
    System.out.println("Not Supported");
  }
}
