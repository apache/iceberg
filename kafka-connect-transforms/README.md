# SMTs for the Apache Iceberg Sink Connector

This project contains some SMTs that could be useful when transforming Kafka data for use by
the Iceberg sink connector.

# CopyValue

The `CopyValue` SMT copies a value from one field to a new field.

## Configuration

| Property         | Description       |
|------------------|-------------------|
| source.field     | Source field name |
| target.field     | Target field name |

## Example

```sql
"transforms": "copyId",
"transforms.copyId.type": "io.tabular.iceberg.connect.transforms.CopyValue",
"transforms.copyId.source.field": "id",
"transforms.copyId.target.field": "id_copy",
```
