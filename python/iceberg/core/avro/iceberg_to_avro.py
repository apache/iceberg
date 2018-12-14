
class IcebergToAvro(object):

    @staticmethod
    def read_avro_row(iceberg_schema, avro_reader):
        try:
            avro_row = avro_reader.__next__()
            iceberg_row = dict()
            for field in iceberg_schema.fields:
                iceberg_row[field.name] = IcebergToAvro.get_field_from_avro(avro_row, field)
            yield iceberg_row
        except StopIteration:
            return

    @staticmethod
    def get_field_from_avro(avro_row, field):
        raise RuntimeError("not yet implemented")
        # if field.type_id == None
