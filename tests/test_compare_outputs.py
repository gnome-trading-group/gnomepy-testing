"""
Unit tests for compare_outputs module.
"""
import tempfile
from pathlib import Path

import pytest
from gnomepy import MBP10, BidAskPair

from gnomepy_testing.compare_outputs import (
    compare_messages,
    compare_files,
    ComparisonResult
)


@pytest.fixture
def sample_mbp10():
    """Create a sample MBP10 message."""
    levels = [
        BidAskPair(
            bid_px=100000000,
            ask_px=101000000,
            bid_sz=1000000,
            ask_sz=2000000,
            bid_ct=5,
            ask_ct=3
        )
    ] + [BidAskPair(0, 0, 0, 0, 0, 0)] * 9
    
    return MBP10(
        exchange_id=1,
        security_id=1,
        timestamp_event=1234567890000000,
        sequence=100,
        timestamp_sent=None,
        timestamp_recv=9999999999999999,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )


def test_compare_identical_messages(sample_mbp10):
    """Test that identical messages compare successfully."""
    result = ComparisonResult()

    compare_messages(sample_mbp10, sample_mbp10, 1, set(), result)

    assert len(result.mismatched_messages) == 0


def test_compare_messages_ignore_timestamp_recv(sample_mbp10):
    """Test that timestamp_recv is properly ignored."""
    msg1 = sample_mbp10

    levels = [
        BidAskPair(
            bid_px=100000000,
            ask_px=101000000,
            bid_sz=1000000,
            ask_sz=2000000,
            bid_ct=5,
            ask_ct=3
        )
    ] + [BidAskPair(0, 0, 0, 0, 0, 0)] * 9

    msg2 = MBP10(
        exchange_id=1,
        security_id=1,
        timestamp_event=1234567890000000,
        sequence=100,
        timestamp_sent=None,
        timestamp_recv=8888888888888888,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )

    result = ComparisonResult()

    compare_messages(msg1, msg2, 1, {'timestamp_recv'}, result)

    assert len(result.mismatched_messages) == 0


def test_compare_messages_with_difference(sample_mbp10):
    """Test that differences are detected."""
    msg1 = sample_mbp10

    levels = [
        BidAskPair(
            bid_px=100000000,
            ask_px=101000000,
            bid_sz=1000000,
            ask_sz=2000000,
            bid_ct=5,
            ask_ct=3
        )
    ] + [BidAskPair(0, 0, 0, 0, 0, 0)] * 9

    msg2 = MBP10(
        exchange_id=1,
        security_id=1,
        timestamp_event=1234567890000000,
        sequence=101,
        timestamp_sent=None,
        timestamp_recv=9999999999999999,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )

    result = ComparisonResult()

    compare_messages(msg1, msg2, 1, set(), result)

    assert len(result.mismatched_messages) == 1
    assert 1 in result.mismatched_messages
    assert len(result.mismatched_messages[1]['field_diffs']) == 1
    assert result.mismatched_messages[1]['field_diffs'][0]['field'] == 'sequence'
    assert result.mismatched_messages[1]['field_diffs'][0]['python'] == 100
    assert result.mismatched_messages[1]['field_diffs'][0]['java'] == 101


def test_compare_files_identical():
    """Test comparing two identical binary files."""
    levels = [
        BidAskPair(
            bid_px=100000000,
            ask_px=101000000,
            bid_sz=1000000,
            ask_sz=2000000,
            bid_ct=5,
            ask_ct=3
        )
    ] + [BidAskPair(0, 0, 0, 0, 0, 0)] * 9
    
    msg1 = MBP10(
        exchange_id=1,
        security_id=1,
        timestamp_event=1234567890000000,
        sequence=100,
        timestamp_sent=None,
        timestamp_recv=1111111111111111,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )
    
    msg2 = MBP10(
        exchange_id=1,
        security_id=1,
        timestamp_event=1234567890000000,
        sequence=100,
        timestamp_sent=None,
        timestamp_recv=2222222222222222,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )
    
    with tempfile.TemporaryDirectory() as tmpdir:
        python_file = Path(tmpdir) / "python.bin"
        java_file = Path(tmpdir) / "java.bin"
        
        with open(python_file, 'wb') as f:
            f.write(msg1.encode())
        
        with open(java_file, 'wb') as f:
            f.write(msg2.encode())
        
        result = compare_files(python_file, java_file, {'timestamp_recv'})

        assert result.is_success()
        assert result.total_messages == 1
        assert len(result.mismatched_messages) == 0


def test_compare_files_with_difference():
    """Test comparing two different binary files."""
    levels = [
        BidAskPair(
            bid_px=100000000,
            ask_px=101000000,
            bid_sz=1000000,
            ask_sz=2000000,
            bid_ct=5,
            ask_ct=3
        )
    ] + [BidAskPair(0, 0, 0, 0, 0, 0)] * 9
    
    msg1 = MBP10(
        exchange_id=1,
        security_id=1,
        timestamp_event=1234567890000000,
        sequence=100,
        timestamp_sent=None,
        timestamp_recv=1111111111111111,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )
    
    msg2 = MBP10(
        exchange_id=1,
        security_id=2,
        timestamp_event=1234567890000000,
        sequence=100,
        timestamp_sent=None,
        timestamp_recv=2222222222222222,
        price=100000000,
        size=1000000,
        action="Modify",
        side="None",
        flags=["marketByPrice"],
        depth=0,
        levels=levels
    )
    
    with tempfile.TemporaryDirectory() as tmpdir:
        python_file = Path(tmpdir) / "python.bin"
        java_file = Path(tmpdir) / "java.bin"
        
        with open(python_file, 'wb') as f:
            f.write(msg1.encode())
        
        with open(java_file, 'wb') as f:
            f.write(msg2.encode())
        
        result = compare_files(python_file, java_file, {'timestamp_recv'})

        assert not result.is_success()
        assert result.total_messages == 1
        assert len(result.mismatched_messages) == 1
        assert 1 in result.mismatched_messages
        assert result.mismatched_messages[1]['field_diffs'][0]['field'] == 'security_id'


def test_comparison_result_is_success():
    """Test ComparisonResult.is_success() logic."""
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

