"""
Unit tests for compare_outputs module.
"""
import pytest

from gnomepy_testing.compare_outputs import (
    compare_messages,
    ComparisonResult
)


class _TestSchema:
    """Minimal schema stub for testing comparison logic — no JVM required."""

    def __init__(self, **fields):
        self._fields = fields

    def to_dict(self):
        return dict(self._fields)


def _base(**overrides):
    return {
        "exchange_id": 1,
        "security_id": 1,
        "timestamp_event": 1234567890000000,
        "sequence": 100,
        "timestamp_sent": 0,
        "timestamp_recv": 9999999999999999,
        "price": 100000000,
        "size": 1000000,
        "action": "Modify",
        "side": "None_",
        "depth": 0,
        **overrides,
    }


@pytest.fixture
def sample_schema():
    return _TestSchema(**_base())


def test_compare_identical_messages(sample_schema):
    result = ComparisonResult()
    compare_messages(sample_schema, sample_schema, 1, set(), result)
    assert len(result.mismatched_messages) == 0


def test_compare_messages_ignore_timestamp_recv(sample_schema):
    msg2 = _TestSchema(**_base(timestamp_recv=8888888888888888))
    result = ComparisonResult()
    compare_messages(sample_schema, msg2, 1, {'timestamp_recv'}, result)
    assert len(result.mismatched_messages) == 0


def test_compare_messages_with_difference(sample_schema):
    msg2 = _TestSchema(**_base(sequence=101))
    result = ComparisonResult()
    compare_messages(sample_schema, msg2, 1, set(), result)
    assert len(result.mismatched_messages) == 1
    assert 1 in result.mismatched_messages
    assert len(result.mismatched_messages[1]['field_diffs']) == 1
    assert result.mismatched_messages[1]['field_diffs'][0]['field'] == 'sequence'
    assert result.mismatched_messages[1]['field_diffs'][0]['python'] == 100
    assert result.mismatched_messages[1]['field_diffs'][0]['java'] == 101


def test_comparison_result_is_success():
    result = ComparisonResult()
    assert result.is_success()

    result.add_mismatch(1, 'field', 'val1', 'val2')
    assert not result.is_success()

    result2 = ComparisonResult()
    result2.python_only_messages = 1
    assert not result2.is_success()

    result3 = ComparisonResult()
    result3.java_only_messages = 1
    assert not result3.is_success()
