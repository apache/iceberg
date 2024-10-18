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
package org.apache.iceberg.flink.source;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.reader.RowConverter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergSourceBoundedRow {
    @TempDir
    protected Path temporaryFolder;

    @RegisterExtension
    private static final HadoopCatalogExtension CATALOG_EXTENSION =
            new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

    @Parameters(name = "format={0}, parallelism = {1}, useConverter = {2}")
    public static Object[][] parameters() {
        return new Object[][]{
                {FileFormat.AVRO, 2, true},
                {FileFormat.PARQUET, 2, true},
                {FileFormat.PARQUET, 2, false},
                {FileFormat.ORC, 2, true}
        };
    }

    @Parameter(index = 0)
    private FileFormat fileFormat;

    @Parameter(index = 1)
    private int parallelism;

    @Parameter(index = 2)
    private boolean useConverter;

    @TestTemplate
    public void testUnpartitionedTable() throws Exception {
        Table table =
                CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
        List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
        new GenericAppenderHelper(table, fileFormat, temporaryFolder).appendToTable(expectedRecords);
        TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
    }

    @TestTemplate
    public void testPartitionedTable() throws Exception {
        String dateStr = "2020-03-20";
        Table table =
                CATALOG_EXTENSION
                        .catalog()
                        .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
        List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
        for (int i = 0; i < expectedRecords.size(); ++i) {
            expectedRecords.get(i).setField("dt", dateStr);
        }

        new GenericAppenderHelper(table, fileFormat, temporaryFolder)
                .appendToTable(org.apache.iceberg.TestHelpers.Row.of(dateStr, 0), expectedRecords);
        TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
    }

    @TestTemplate
    public void testProjection() throws Exception {
        Table table =
                CATALOG_EXTENSION
                        .catalog()
                        .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
        List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
        new GenericAppenderHelper(table, fileFormat, temporaryFolder)
                .appendToTable(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
        // select the "data" field (fieldId == 1)
        Schema projectedSchema = TypeUtil.select(TestFixtures.SCHEMA, Sets.newHashSet(1));
        List<Row> expectedRows =
                Arrays.asList(Row.of(expectedRecords.get(0).get(0)), Row.of(expectedRecords.get(1).get(0)));
        TestHelpers.assertRows(
                run(projectedSchema, Collections.emptyList(), Collections.emptyMap()), expectedRows);
    }

    private List<Row> run() throws Exception {
        return run(null, Collections.emptyList(), Collections.emptyMap());
    }

    private List<Row> run(
            Schema projectedSchema, List<Expression> filters, Map<String, String> options)
            throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().enableObjectReuse();

        Configuration config = new Configuration();
        config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 128);
        Table table;
        try (TableLoader tableLoader = CATALOG_EXTENSION.tableLoader()) {
            tableLoader.open();
            table = tableLoader.loadTable();
        }

        Schema readSchema = projectedSchema != null ? projectedSchema : table.schema();
        IcebergSource.Builder<Row> sourceBuilder = createSourceBuilderWithConverter(readSchema, config);

        if (projectedSchema != null) {
            sourceBuilder.project(projectedSchema);
        }

        sourceBuilder.filters(filters);
        sourceBuilder.setAll(options);

        RowType rowType = FlinkSchemaUtil.convert(readSchema);

        DataStream<Row> stream =
                env.fromSource(
                        sourceBuilder.build(),
                        WatermarkStrategy.noWatermarks(),
                        "testBasicRead",
                        getTypeInfo(rowType));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.createTemporaryView("testBasicRead", stream);
        TableResult tableResult = tableEnvironment.sqlQuery("select * from testBasicRead").execute();
        tableResult.print();


        try (CloseableIterator<Row> iter = stream.executeAndCollect()) {
            return Lists.newArrayList(iter);
        }
    }

    private IcebergSource.Builder<Row> createSourceBuilderWithConverter(
            Schema readSchema, Configuration config) {
        RowConverter converter =
                RowConverter.fromIcebergSchema(readSchema);
        return IcebergSource.forOutputType(converter)
                .tableLoader(CATALOG_EXTENSION.tableLoader())
                .assignerFactory(new SimpleSplitAssignerFactory())
                .flinkConfig(config);
    }

    private static RowTypeInfo getTypeInfo(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[fieldCount];
        String[] fieldNames = new String[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            LogicalType logicalType = rowType.getTypeAt(i);
            fieldTypes[i] = TypeInformation.of(logicalType.getDefaultConversion());
            fieldNames[i] = rowType.getFieldNames().get(i);
        }
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

}
