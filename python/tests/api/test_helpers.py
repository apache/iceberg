import io
import pickle

from iceberg.api.data_file import DataFile
from iceberg.api.expressions.expressions import ExpressionVisitors
from iceberg.api.expressions.predicate import (BoundPredicate,
                                               UnboundPredicate)
from iceberg.api.struct_like import StructLike
from nose.tools import assert_true


class TestHelpers(object):

    @staticmethod
    def assert_all_references_bound(message, expr):
        ExpressionVisitors.visit(expr, TestHelpers.CheckReferencesBound(message))

    @staticmethod
    def assert_and_unwrap(expr, expected=None):
        if expected is not None:
            assert_true(isinstance(expr, expected))
        else:
            assert_true(isinstance(expr, BoundPredicate))

        return expr

    @staticmethod
    def round_trip_serialize(type_var):
        stream = io.BytesIO()
        pickle.dump(type_var, stream, pickle.HIGHEST_PROTOCOL)
        stream.seek(0)

        return pickle.load(stream)

    class Row(StructLike):

        @staticmethod
        def of(values=None):
            return TestHelpers.Row(values)

        def __init__(self, values):
            self.values = values

        def get(self, pos):
            return self.values[pos]

        def set(self, pos, value):
            raise RuntimeError("Setting values is not supported")

    class CheckReferencesBound(ExpressionVisitors.ExpressionVisitor):

            def __init__(self, message):
                self.message = message

            def predicate(self, pred):
                if isinstance(pred, UnboundPredicate):
                    assert_true(False)

                return None


class TestDataFile(DataFile):

    def __init__(self, path, partition, record_count, value_counts=None, null_value_counts=None,
                 lower_bounds=None, upper_bounds=None):
        self.path = path
        self.partition = partition
        self.record_count = record_count
        self.value_counts = value_counts
        self.null_value_counts = null_value_counts
        self.lower_bounds = lower_bounds
        self.upper_bounds = upper_bounds
        self.file_size_in_bytes = 0
        self.block_size_in_bytes = 0
        self.file_ordinal = None
        self.column_size = None

    def copy(self):
        return self
