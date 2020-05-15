package org.apache.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iceberg.types.Types.NestedField.required;

public class PrimaryKeySpec {
  // IDs for pk fields start at 1000
  private static final int PK_DATA_ID_START = 10000;
  public static final String OFFSET_COLUMN = "_iceberg_offset";
  public static final String DEL_COLUMN = "_iceberg_del";

  private Schema schema;
  private final ImmutableList<PrimaryKeyField> pkFields;
  private Schema deltaSchema;

  private PrimaryKeySpec(Schema schema, List<PrimaryKeyField> pkFields) {
    this.schema = schema;
    this.pkFields = ImmutableList.copyOf(pkFields);
    initDeltaSchema();
  }

  private void initDeltaSchema() {
    List<Types.NestedField> columns = Lists.newArrayList(schema.columns());
    int maxId = 0;
    for (Types.NestedField column : columns) {
      if (column.name().equalsIgnoreCase(OFFSET_COLUMN) ||
          column.name().equalsIgnoreCase(DEL_COLUMN))
        throw new ValidationException("can not name a column by _iceberg_offset or _iceberg_del");
      if (column.fieldId() > maxId)
        maxId = column.fieldId();
    }
    columns.add(required(++maxId, OFFSET_COLUMN, Types.LongType.get()));
    columns.add(required(++maxId, DEL_COLUMN, Types.BooleanType.get()));
    this.deltaSchema = new Schema(ImmutableList.copyOf(columns));
  }

  public Schema getSchema() {
    return schema;
  }

  public Schema getDeltaSchema() {
    return deltaSchema;
  }

  public PrimaryKeySpec updateSchema(Schema newSchema) {
    pkFields.stream().forEach(field -> {
      if (newSchema.findField(field.sourceId) == null)
        throw new ValidationException("source id %s,name %s is not found in new schema",
            field.sourceId, field.name);
    });
    this.schema = schema;
    return this;
  }

  /**
   * @return a {@link Types.StructType} for primary key data defined by this spec.
   */
  public Types.StructType primaryKeyType() {
    List<Types.NestedField> structFields = Lists.newArrayListWithExpectedSize(pkFields.size());

    for (PrimaryKeyField field : pkFields) {
      Types.NestedField sourceField = schema.findField(field.sourceId());
      structFields.add(Types.NestedField.optional(field.fieldId, field.name(),
              primaryKeyDataType(sourceField, field.layout)));
    }

    return Types.StructType.of(structFields);
  }

  /**
   * Creates a new {@link PrimaryKeySpec.Builder primary key spec builder} for the given {@link Schema}.
   *
   * @param schema a schema
   * @return a primary key spec builder for the given schema
   */
  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  /**
   * @return the list of {@link PrimaryKeyField primary key fields} for this spec.
   */
  public List<PrimaryKeyField> fields() {
    return pkFields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrimaryKeySpec that = (PrimaryKeySpec) o;
    return pkFields.equals(that.pkFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pkFields);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (PrimaryKeyField field : pkFields) {
      sb.append("\n");
      sb.append("  ").append(field);
    }
    if (pkFields.size() > 0) {
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Used to create valid {@link PrimaryKeySpec specs}.
   * <p>
   * Call {@link #builderFor(Schema)} to create a new builder.
   */
  public static class Builder {
    private final AtomicInteger lastAssignedFieldId = new AtomicInteger(PK_DATA_ID_START - 1);
    private final Schema schema;
    private List<PrimaryKeyField> pkFields = new ArrayList<>();
    private boolean hashInvolved = false;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    private int nextFieldId() {
      return lastAssignedFieldId.incrementAndGet();
    }

    public Builder addColumn(String columnName) {
      addColumn(columnName, PrimaryKeyLayout.RANGE);
      return this;
    }

    public Builder addColumn(Integer columnIndex) {
      addColumn(columnIndex, PrimaryKeyLayout.RANGE);
      return this;
    }

    public Builder addColumn(String columnName, PrimaryKeyLayout layout) {
      Types.NestedField sourceColumn = schema.findField(columnName);
      Preconditions.checkArgument(sourceColumn != null,
          "Cannot find source column: %s", columnName);
      addColumn(sourceColumn, layout);
      return this;
    }

    public Builder addColumn(Integer columnIndex, PrimaryKeyLayout layout) {
      Types.NestedField sourceColumn = schema.findField(columnIndex);
      Preconditions.checkArgument(sourceColumn != null,
          "Cannot find source column by id: %s", columnIndex);
      addColumn(sourceColumn, layout);
      return this;
    }

    public Builder addColumn(Types.NestedField sourceColumn, PrimaryKeyLayout layout) {
      Preconditions.checkArgument(layout != PrimaryKeyLayout.RANGE || !hashInvolved,
          "can not define a range layout key after hash key");
      pkFields.add(new PrimaryKeyField(sourceColumn.name(), sourceColumn.fieldId(), nextFieldId(), layout));
      if (layout == PrimaryKeyLayout.HASH && !hashInvolved)
        hashInvolved = true;
      return this;
    }

    Builder addColumn(int sourceId, String columnName, PrimaryKeyLayout layout) {
      Types.NestedField column = schema.findField(sourceId);
      Preconditions.checkNotNull(column, "Cannot find source column: %s", sourceId);
      checkColumn(columnName, column.fieldId());
      addColumn(column, layout);
      return this;
    }

    private void checkColumn(String name, Integer identitySourceColumnId) {
      Types.NestedField schemaField = schema.findField(name);
      if (identitySourceColumnId != null) {
        Preconditions.checkArgument(schemaField != null && schemaField.fieldId() == identitySourceColumnId,
                "Cannot create identity primary key sourced from different field in schema: %s", name);
      } else {
        // for all other transforms we don't allow conflicts between primary key field name and schema field name
        Preconditions.checkArgument(schemaField == null,
                "Cannot create primary key from name that exists in schema: %s", name);
      }
      Preconditions.checkArgument(name != null && !name.isEmpty(),
              "Cannot use empty or null primary key field name: %s", name);
    }

    public PrimaryKeySpec build() {
      return new PrimaryKeySpec(schema, pkFields);
    }
  }

  public static class PrimaryKeyField {
    private final String name;
    private final Integer sourceId;
    private final int fieldId;
    private final PrimaryKeyLayout layout;

    PrimaryKeyField(String name, Integer sourceId, int fieldId, PrimaryKeyLayout layout) {
      this.name = name;
      this.sourceId = sourceId;
      this.fieldId = fieldId;
      this.layout = layout;
    }

    public String name() {
      return name;
    }

    public Integer fieldId() {
      return fieldId;
    }

    public Integer sourceId() {
      return sourceId;
    }

    public PrimaryKeyLayout layout() {
      return layout;
    }

    @Override
    public String toString() {
      return name + ": " + layout + "(" + sourceId + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PrimaryKeyField that = (PrimaryKeyField) o;
      return name.equals(that.name) &&
              sourceId.equals(that.sourceId) &&
              layout == that.layout;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, sourceId, layout);
    }
  }

  public enum PrimaryKeyLayout {
    HASH,
    RANGE;
  }

  private static final PrimaryKeySpec NO_PRIMARY_KEY_SPEC =
          new PrimaryKeySpec(new Schema(), ImmutableList.of());

  public static PrimaryKeySpec noPrimaryKey() {
    return NO_PRIMARY_KEY_SPEC;
  }

  private static Type primaryKeyDataType(Types.NestedField sourceColumn, PrimaryKeyLayout layout) {
    int filedId = sourceColumn.fieldId();
    switch (layout) {
      case HASH:
        return Types.StructType.of(required(filedId * 10 + 1, "lower_bound", sourceColumn.type()),
                required(filedId * 10 + 2, "upper_bound", sourceColumn.type()));
      case RANGE:
        return Types.StructType.of(required(filedId * 10 + 1, "bucket_number", Types.LongType.get()),
                required(filedId * 10 + 2, "bucket", Types.LongType.get()));
      default:
        throw new IllegalArgumentException("Unknown primary layout :" + layout);
    }
  }
}
