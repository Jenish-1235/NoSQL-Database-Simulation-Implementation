"""
Class responsible to manage the WAL (Write Ahead Log) for whole database service
"""
import datetime

from core import file_io_manager
from typing import Any, Dict


class WALManager:
    def __init__(self):
        self.file_io_manager = file_io_manager.FileIOManager()
        self.wal_file = "D:\\HLD-Assignments\\NoSQL-Database-Simulation-Implementation\\persistent-storage\\wal_file.txt"

    async def update_wal_file(self, wal_data: Dict[str, Any], operation:str):
        wal_record:str = "\n" + str(datetime.datetime.now()) +  " " + operation + " " + wal_data["key"] + " " + wal_data["value"]
        self.file_io_manager.writeWAL(wal_file=self.wal_file, wal_record=wal_record)
        return "Successfully updated WAL"