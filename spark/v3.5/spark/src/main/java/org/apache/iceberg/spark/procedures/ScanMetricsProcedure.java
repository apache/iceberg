package org.apache.iceberg.spark.procedures;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ScanMetricsProcedure extends BaseProcedure {

    private static final ProcedureParameter TABLE_PARAM =
            ProcedureParameter.required("table", DataTypes.StringType);
    private static final ProcedureParameter SNAPSHOT_ID_PARAM =
            ProcedureParameter.optional("snapshot_id", DataTypes.LongType);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[]{TABLE_PARAM, SNAPSHOT_ID_PARAM};

    private static final StructType OUTPUT_TYPE = new StructType(
            new StructField[]{
                    new StructField("total_data_records", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("total_data_files", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("records_with_no_deletes", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("records_with_only_eq_deletes", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("records_with_only_pos_deletes", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("records_with_both_deletes", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("records_with_eq_deletes_total", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("unique_eq_delete_files", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("eq_delete_files_referenced", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("eq_delete_records", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("unique_pos_delete_files", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("pos_delete_files_referenced", DataTypes.LongType, false, Metadata.empty()),
                    new StructField("pos_delete_records", DataTypes.LongType, false, Metadata.empty())
            });

    public static Builder<ScanMetricsProcedure> builder() {
        return new Builder<ScanMetricsProcedure>() {
            @Override
            protected ScanMetricsProcedure doBuild() {
                return new ScanMetricsProcedure(tableCatalog());
            }
        };
    }

    private ScanMetricsProcedure(TableCatalog catalog) {
        super(catalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public String description() {
        return "Collects metrics about data files and their associated delete files";
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
        Identifier ident = input.ident(TABLE_PARAM);
        Long snapshotId = input.asLong(SNAPSHOT_ID_PARAM, null);

        return withIcebergTable(ident, icebergTable -> {
            TableScan scan = icebergTable.newScan();
            if (snapshotId != null) {
                scan = scan.useSnapshot(snapshotId);
            }

            CloseableIterable<FileScanTask> tasks = scan.planFiles();

            AtomicLong totalDataRecords = new AtomicLong(0);
            AtomicLong dataRecordsWithNoDeletes = new AtomicLong(0);
            AtomicLong dataRecordsWithOnlyEqDeletes = new AtomicLong(0);
            AtomicLong dataRecordsWithOnlyPosDeletes = new AtomicLong(0);
            AtomicLong dataRecordsWithBothDeletes = new AtomicLong(0);
            Set<String> uniqEqDeleteFiles = Sets.newHashSet();
            Set<String> uniqPosDeleteFiles = Sets.newHashSet();
            AtomicLong totalEqDeleteRecords = new AtomicLong(0);
            AtomicLong totalPosDeleteRecords = new AtomicLong(0);
            AtomicLong eqDeleteFilesReferenced = new AtomicLong(0);
            AtomicLong posDeleteFilesReferenced = new AtomicLong(0);
            AtomicLong totalDataFiles = new AtomicLong(0);

            for (FileScanTask task : tasks) {
                DataFile dataFile = task.file();
                List<DeleteFile> deleteFiles = task.deletes();
                totalDataFiles.incrementAndGet();

                long eqDeleteFileCount = 0;
                long posDeleteFileCount = 0;

                for (DeleteFile deleteFile : deleteFiles) {
                    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                        eqDeleteFileCount++;
                        eqDeleteFilesReferenced.incrementAndGet();
                        if (uniqEqDeleteFiles.add(deleteFile.path().toString())) {
                            totalEqDeleteRecords.addAndGet(deleteFile.recordCount());
                        }
                    } else if (deleteFile.content() == FileContent.POSITION_DELETES) {
                        posDeleteFileCount++;
                        posDeleteFilesReferenced.incrementAndGet();
                        if (uniqPosDeleteFiles.add(deleteFile.path().toString())) {
                            totalPosDeleteRecords.addAndGet(deleteFile.recordCount());
                        }
                    }
                }

                long dataRecordCount = dataFile.recordCount();
                totalDataRecords.addAndGet(dataRecordCount);

                if (eqDeleteFileCount == 0 && posDeleteFileCount == 0) {
                    dataRecordsWithNoDeletes.addAndGet(dataRecordCount);
                } else if (eqDeleteFileCount > 0 && posDeleteFileCount == 0) {
                    dataRecordsWithOnlyEqDeletes.addAndGet(dataRecordCount);
                } else if (eqDeleteFileCount == 0 && posDeleteFileCount > 0) {
                    dataRecordsWithOnlyPosDeletes.addAndGet(dataRecordCount);
                } else {
                    dataRecordsWithBothDeletes.addAndGet(dataRecordCount);
                }
            }

            long dataRecordsWithEqDeletesTotal =
                    dataRecordsWithOnlyEqDeletes.get() + dataRecordsWithBothDeletes.get();

            return new InternalRow[]{new GenericInternalRow(new Object[]{
                    totalDataRecords.get(),
                    totalDataFiles.get(),
                    dataRecordsWithNoDeletes.get(),
                    dataRecordsWithOnlyEqDeletes.get(),
                    dataRecordsWithOnlyPosDeletes.get(),
                    dataRecordsWithBothDeletes.get(),
                    dataRecordsWithEqDeletesTotal,
                    uniqEqDeleteFiles.size(),
                    eqDeleteFilesReferenced.get(),
                    totalEqDeleteRecords.get(),
                    uniqPosDeleteFiles.size(),
                    posDeleteFilesReferenced.get(),
                    totalPosDeleteRecords.get()
            })};
        });
    }
}
