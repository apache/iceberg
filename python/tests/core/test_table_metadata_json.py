import json
import unittest

from iceberg.core import TableMetadataParser


TABLE_METADATA = {'format-version': 1,
                  'location': 's3n://some/path/to/sample_partitioned',
                  'last-updated-ms': 1544463150134,
                  'last-column-id': 4,
                  'schema': {'type': 'struct',
                             'fields': [{'id': 1, 'name': 'i', 'required': False, 'type': 'int'},
                                        {'id': 2, 'name': 's', 'required': False, 'type': 'string'},
                                        {'id': 3, 'name': 'p1', 'required': False, 'type': 'int'},
                                        {'id': 4, 'name': 'p2', 'required': False, 'type': 'int'}]},
                  'partition-spec': [{'name': 'p1', 'transform': 'identity', 'source-id': 3},
                                     {'name': 'p2', 'transform': 'identity', 'source-id': 4}],
                  'default-spec-id': 0,
                  'partition-specs': [{'spec-id': 0,
                                       'fields': [{'name': 'p1', 'transform': 'identity', 'source-id': 3},
                                                  {'name': 'p2', 'transform': 'identity', 'source-id': 4}]}],
                  'properties': {},
                  'current-snapshot-id': -1,
                  'snapshots': [],
                  'snapshot-log': []}


class TestTableMetadataJson(unittest.TestCase):

    def test_roundtrip_conversion(self):
        table_metadata_str = json.dumps(TABLE_METADATA)
        schema_out = TableMetadataParser.from_json(None, None, table_metadata_str)

        self.assertEqual(TableMetadataParser.to_json(schema_out), table_metadata_str)
