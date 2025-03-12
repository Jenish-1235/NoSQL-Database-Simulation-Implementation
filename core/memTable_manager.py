"""
Class responsible to manage the memTable for whole database service
"""
from typing import Dict, Any

from core.file_io_manager import FileIOManager
from core.ssTable_manager import SSTableManager
from schemas.sorted_set_table import SortedSetTable


class memTableManager:
    def __init__(self):
        self.wal_file = "D:\\HLD-Assignments\\NoSQL-Database-Simulation-Implementation\\persistent-storage\\wal_file.txt"
        self.file_io_manager = FileIOManager()
        self.ssTableManager = SSTableManager()

    def reconstructMemTable(self):

        last_ssTable_id = self.ssTableManager.retrieve_last_id_from_id_file()
        final_ssTable = SortedSetTable
        if last_ssTable_id is not None:
            ssTableList = self.ssTableManager.read_sstables(last_ssTable_id)
            for ssTable in ssTableList:
                if ssTable.SSTableId == last_ssTable_id:
                    final_ssTable = ssTable

        lastSSTable_timeStamp = final_ssTable.timestamp
        self.wal_file = self.file_io_manager.openWAL(self.wal_file)
        reconstructed_mem_table = {}
        i = 0
        for record in self.wal_file:
            if record != "":
                if record == "\n":
                    continue
                list_of_strings = record.split(" ")
                timeStamp = list_of_strings[0] + " " + list_of_strings[1]
                if timeStamp > lastSSTable_timeStamp:
                    operation = list_of_strings[2]
                    key = list_of_strings[3]
                    value = list_of_strings[4].rstrip("\n")
                    reconstructed_mem_table[operation + str(i)] = {"key": key, "value": value}
                    i+=1
            else:
                return {}
        print(reconstructed_mem_table)
        return reconstructed_mem_table

memTableManager = memTableManager()
memTableManager.reconstructMemTable()