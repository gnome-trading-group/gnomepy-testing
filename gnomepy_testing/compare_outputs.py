"""
Compare binary outputs from Python and Java clients.

This script:
1. Loads binary files using gnomepy DataStore
2. Iterates through messages from both files
3. Compares field-by-field, ignoring non-deterministic fields
4. Reports differences and summary
"""
import argparse
import sys
from pathlib import Path
from typing import Any

from gnomepy import DataStore, MBP10, MBP1, SchemaType


class ComparisonResult:
    """Tracks comparison results."""

    def __init__(self):
        self.total_messages = 0
        self.mismatched_messages = {}
        self.python_only_messages = 0
        self.java_only_messages = 0

    def add_mismatch(self, message_num: int, field: str, python_val: Any, java_val: Any,
                    python_msg: Any = None, java_msg: Any = None):
        """Record a field mismatch."""
        if message_num not in self.mismatched_messages:
            self.mismatched_messages[message_num] = {
                'python_msg': python_msg,
                'java_msg': java_msg,
                'field_diffs': []
            }

        self.mismatched_messages[message_num]['field_diffs'].append({
            'field': field,
            'python': python_val,
            'java': java_val
        })

    def is_success(self) -> bool:
        """Check if comparison passed."""
        return (len(self.mismatched_messages) == 0 and
                self.python_only_messages == 0 and
                self.java_only_messages == 0)

    def total_field_mismatches(self) -> int:
        """Get total number of field mismatches across all messages."""
        return sum(len(msg['field_diffs']) for msg in self.mismatched_messages.values())


def compare_messages(python_msg: Any, java_msg: Any, message_num: int,
                    ignore_fields: set[str], result: ComparisonResult):
    """
    Compare two messages field-by-field.

    Args:
        python_msg: Python message object
        java_msg: Java message object
        message_num: Message number for reporting
        ignore_fields: Set of field names to ignore
        result: ComparisonResult to update
    """
    if type(python_msg) != type(java_msg):
        result.add_mismatch(message_num, '__type__',
                          type(python_msg).__name__,
                          type(java_msg).__name__,
                          python_msg, java_msg)
        return

    has_mismatch = False
    for field_name in dir(python_msg):
        if field_name.startswith('_'):
            continue
        if field_name in ignore_fields:
            continue
        if isinstance(getattr(type(python_msg), field_name, None), property):
            continue
        if callable(getattr(python_msg, field_name)):
            continue

        python_val = getattr(python_msg, field_name)
        java_val = getattr(java_msg, field_name)

        if python_val != java_val:
            if not has_mismatch:
                has_mismatch = True
            result.add_mismatch(message_num, field_name, python_val, java_val,
                              python_msg, java_msg)


def compare_files(python_file: Path, java_file: Path, 
                 ignore_fields: set[str]) -> ComparisonResult:
    """
    Compare two binary output files.
    
    Args:
        python_file: Path to Python output file
        java_file: Path to Java output file
        ignore_fields: Set of field names to ignore in comparison
        
    Returns:
        ComparisonResult with comparison details
    """
    result = ComparisonResult()
    
    with open(python_file, 'rb') as f:
        python_bytes = f.read()
    
    with open(java_file, 'rb') as f:
        java_bytes = f.read()
    
    python_store = DataStore.from_bytes(python_bytes, SchemaType.MBP_10)
    java_store = DataStore.from_bytes(java_bytes, SchemaType.MBP_10)
    
    python_messages = [msg for msg in python_store]
    java_messages = [msg for msg in java_store]
    
    result.total_messages = min(len(python_messages), len(java_messages))
    result.python_only_messages = max(0, len(python_messages) - len(java_messages))
    result.java_only_messages = max(0, len(java_messages) - len(python_messages))
    
    for i, (python_msg, java_msg) in enumerate(zip(python_messages, java_messages), 1):
        compare_messages(python_msg, java_msg, i, ignore_fields, result)
    
    return result


def _get_message_context(msg: Any) -> dict[str, Any]:
    """Extract key identifying fields from a message for context."""
    context = {}

    for field in ['sequence', 'timestamp_event', 'exchange_id', 'security_id', 'action', 'side']:
        if hasattr(msg, field):
            context[field] = getattr(msg, field)

    return context


def print_results(result: ComparisonResult):
    """Print comparison results."""
    print()
    print("=" * 80)
    print("Comparison Results")
    print("=" * 80)
    print(f"Total messages compared: {result.total_messages}")

    if result.python_only_messages > 0:
        print(f"Python-only messages: {result.python_only_messages}")

    if result.java_only_messages > 0:
        print(f"Java-only messages: {result.java_only_messages}")

    if len(result.mismatched_messages) > 0:
        total_field_mismatches = result.total_field_mismatches()
        print(f"\nFound {len(result.mismatched_messages)} mismatched message(s) with {total_field_mismatches} field difference(s):")
        print()

        messages_to_show = list(result.mismatched_messages.items())[:10]

        for msg_num, mismatch_info in messages_to_show:
            python_msg = mismatch_info['python_msg']
            java_msg = mismatch_info['java_msg']
            field_diffs = mismatch_info['field_diffs']

            print(f"Message #{msg_num}:")

            if python_msg and java_msg:
                python_context = _get_message_context(python_msg)
                java_context = _get_message_context(java_msg)

                if python_context.get('sequence') == java_context.get('sequence'):
                    print(f"  Sequence: {python_context.get('sequence', 'N/A')}")
                else:
                    print(f"  Sequence - Python: {python_context.get('sequence', 'N/A')}, Java: {java_context.get('sequence', 'N/A')}")

                if python_context.get('timestamp_event'):
                    print(f"  Timestamp: {python_context.get('timestamp_event', 'N/A')}")

                if python_context.get('action'):
                    print(f"  Action: {python_context.get('action', 'N/A')}")

            print(f"  Field differences ({len(field_diffs)}):")
            for diff in field_diffs:
                print(f"    • {diff['field']}:")
                print(f"        Python: {diff['python']}")
                print(f"        Java:   {diff['java']}")
            print()

        if len(result.mismatched_messages) > 10:
            remaining = len(result.mismatched_messages) - 10
            remaining_fields = sum(len(result.mismatched_messages[msg_num]['field_diffs'])
                                 for msg_num in list(result.mismatched_messages.keys())[10:])
            print(f"... and {remaining} more mismatched message(s) with {remaining_fields} field difference(s)")

    print()
    print("=" * 80)
    if result.is_success():
        print("✓ PASS: Outputs match!")
    else:
        print("✗ FAIL: Outputs differ")
    print("=" * 80)
    print()


def main():
    parser = argparse.ArgumentParser(description='Compare Python and Java binary outputs')
    parser.add_argument('--python', '-p', type=Path, required=True,
                       help='Path to Python output file')
    parser.add_argument('--java', '-j', type=Path, required=True,
                       help='Path to Java output file')
    parser.add_argument('--ignore-fields', '-i', type=str, nargs='*',
                       default=['timestamp_recv'],
                       help='Fields to ignore in comparison (default: timestamp_recv)')
    
    args = parser.parse_args()
    
    if not args.python.exists():
        print(f"ERROR: Python output file not found: {args.python}")
        sys.exit(1)
    
    if not args.java.exists():
        print(f"ERROR: Java output file not found: {args.java}")
        sys.exit(1)
    
    print(f"Comparing outputs:")
    print(f"  Python: {args.python}")
    print(f"  Java:   {args.java}")
    print(f"  Ignoring fields: {', '.join(args.ignore_fields)}")
    
    ignore_fields = set(args.ignore_fields)
    result = compare_files(args.python, args.java, ignore_fields)
    
    print_results(result)
    
    sys.exit(0 if result.is_success() else 1)


if __name__ == '__main__':
    main()

