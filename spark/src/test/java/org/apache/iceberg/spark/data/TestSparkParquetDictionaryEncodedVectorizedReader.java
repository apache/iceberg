package org.apache.iceberg.spark.data;

import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestSparkParquetDictionaryEncodedVectorizedReader extends TestSparkParquetVectorizedReader {

    @Override
    protected void writeAndValidate(Schema schema) throws IOException {
        // Write test data
        Assume.assumeTrue("Parquet Avro cannot write non-string map keys", null == TypeUtil.find(schema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

        List<GenericData.Record> expected = DictionaryData.generateDictionaryEncodableData(schema, 100000, 0L);

        // write a test parquet file using iceberg writer
        File testFile = temp.newFile();
        Assert.assertTrue("Delete should succeed", testFile.delete());

        try (FileAppender<GenericData.Record> writer = Parquet.write(Files.localOutput(testFile))
                .schema(schema)
                .named("test")
                .build()) {
            writer.addAll(expected);
        }
        assertRecordsMatch(schema, expected, testFile);
    }
}
