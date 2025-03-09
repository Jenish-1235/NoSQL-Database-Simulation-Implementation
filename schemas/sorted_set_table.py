"""
Schema for sorted-set-table
"""
from typing import Any

from pydantic import BaseModel


class SortedSetTable(BaseModel):
    SSTableId : str
    nextSSTableId : str
    timestamp : str
    data: Any