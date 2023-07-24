# SMTs for the Apache Iceberg Sink Connector

This project contains some SMTs that could be useful when transforming Kafka data for use by
the Iceberg sink connector.

# CopyValue
_(Experimental)_

The `CopyValue` SMT copies a value from one field to a new field.

## Configuration

| Property         | Description       |
|------------------|-------------------|
| source.field     | Source field name |
| target.field     | Target field name |

## Example

```
"transforms": "copyId",
"transforms.copyId.type": "io.tabular.iceberg.connect.transforms.CopyValue",
"transforms.copyId.source.field": "id",
"transforms.copyId.target.field": "id_copy",
```

# DmsTransform
_(Experimental)_

The `DmsTransform` SMT transforms an AWS DMS formatted message for use by the sink's CDC feature.
It will promote the `data` element fields to top level and add the following metadata fields:
`_cdc_op`, `_cdc_ts`, and `_cdc_table`.

## Configuration

The SMT currently has no configuration. It can be used with the sink's CDC feature, e.g.
```
"iceberg.tables.cdcField": "_cdc_op",
```
