"""
CSV streaming utilities for large dataset exports.

Streams data to avoid loading entire dataset into memory.
"""

import csv
from io import StringIO
from typing import List, Any, Iterator
import logging

logger = logging.getLogger(__name__)


class CSVStreamer:
    """
    Utility for streaming query results as CSV.
    
    Efficiently handles large datasets by streaming rows
    instead of loading everything into memory.
    """
    
    def __init__(self, headers: List[str]):
        """
        Initialize CSV streamer.
        
        Args:
            headers: Column headers for CSV
        """
        self.headers = headers
    
    def stream_rows(self, query_results: Iterator[Any]) -> Iterator[str]:
        """
        Stream database query results as CSV rows.
        
        Yields CSV-formatted strings one at a time, avoiding
        memory issues with large datasets.
        
        Args:
            query_results: Iterator of database query results (SQLAlchemy models)
            
        Yields:
            str: CSV-formatted rows
        """
        # Create StringIO buffer for CSV writing
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header row
        writer.writerow(self.headers)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)
        
        # Stream data rows
        row_count = 0
        for row in query_results:
            # Convert SQLAlchemy model to dict
            if hasattr(row, '__dict__'):
                row_dict = {k: v for k, v in row.__dict__.items() if not k.startswith('_')}
            else:
                row_dict = dict(row)
            
            # Extract values in header order
            values = []
            for header in self.headers:
                value = row_dict.get(header, '')
                
                # Handle None values
                if value is None:
                    values.append('')
                # Handle date/datetime objects
                elif hasattr(value, 'isoformat'):
                    values.append(value.isoformat())
                # Handle decimal/numeric types
                elif hasattr(value, '__float__'):
                    values.append(str(value))
                else:
                    values.append(str(value))
            
            # Write row
            writer.writerow(values)
            yield output.getvalue()
            
            # Clear buffer
            output.seek(0)
            output.truncate(0)
            
            row_count += 1
            
            # Log progress for large exports
            if row_count % 1000 == 0:
                logger.info(f"Streamed {row_count} CSV rows")
        
        logger.info(f"CSV streaming complete: {row_count} total rows")


def generate_filename(dataset_name: str, extension: str = "csv") -> str:
    """
    Generate a timestamped filename for downloads.
    
    Args:
        dataset_name: Name of the dataset
        extension: File extension (default: csv)
    
    Returns:
        str: Filename with timestamp (e.g., "building_permits_20240115_103045.csv")
    """
    from datetime import datetime
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = dataset_name.lower().replace('-', '_').replace(' ', '_')
    
    return f"{safe_name}_{timestamp}.{extension}"