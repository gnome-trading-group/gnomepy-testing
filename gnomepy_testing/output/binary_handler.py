"""
Binary output handler - writes gnomepy schema objects as raw bytes.

This handler takes gnomepy schema objects (MBP1, MBP10, etc.) and writes
them to a file in their binary encoded format for comparison testing.
"""
import logging
from pathlib import Path
from typing import BinaryIO

from gnomepy import SchemaBase

logger = logging.getLogger(__name__)


class BinaryOutputHandler:
    """
    Writes gnomepy schema objects to a binary file.
    
    Each schema object is encoded using its .encode() method and written
    sequentially to the output file.
    """
    
    def __init__(self, output_path: Path):
        """
        Initialize the binary output handler.
        
        Args:
            output_path: Path to the output file
        """
        self.output_path = Path(output_path)
        self.file: BinaryIO | None = None
        self.message_count = 0

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Binary output handler initialized: {self.output_path}")
    
    def open(self):
        """Open the output file for writing."""
        if self.file is None:
            self.file = open(self.output_path, 'wb')
            logger.info(f"Opened binary output file: {self.output_path}")
    
    def write(self, schema_obj: SchemaBase):
        """Write a schema object to the output file."""
        if self.file is None:
            self.open()

        try:
            encoded_bytes = schema_obj.encode()
            self.file.write(encoded_bytes)
            self.message_count += 1

            if self.message_count % 1000 == 0:
                logger.debug(f"Wrote {self.message_count} messages")

        except Exception as e:
            logger.error(f"Failed to write schema object: {e}")
            raise
    
    def flush(self):
        """Flush the output file."""
        if self.file:
            self.file.flush()
    
    def close(self):
        """Close the output file."""
        if self.file:
            self.file.close()
            self.file = None
            logger.info(f"Closed binary output file: {self.output_path} ({self.message_count} messages)")
    
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

