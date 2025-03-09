"""
Class responsible to manage the memTable for whole database service
"""
from typing import Dict, Any

from core.file_io_manager import FileIOManager


class memTableManager:
    def __init__(self):
        self.wal_file = "D:\\HLD-Assignments\\NoSQL-Database-Simulation-Implementation\\persistent-storage\\wal_file.txt"
        self.file_io_manager = FileIOManager()

    def reconstructMemTable(self):
        self.wal_file = self.file_io_manager.openWAL(self.wal_file)
        reconstructed_mem_table = {}

        for record in self.wal_file:

            if record != "":
                if record == "\n":
                    continue
                list_of_strings = record.split(" ")
                operation = list_of_strings[2]
                key = list_of_strings[3]
                value = list_of_strings[4]
                reconstructed_mem_table[operation] = {"key": key, "value": value}
            else:
                return {}

        return reconstructed_mem_table