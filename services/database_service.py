"""
Class responsible as service for handling all the NoSQL CRUD Tasks...
"""
import asyncio
from datetime import datetime
from typing import Dict

from core.memTable_manager import memTableManager
import sys
from core.ssTable_manager import SSTableManager

from schemas.sorted_set_table import SortedSetTable


class DatabaseService:

    async def dump_memtable_to_sstable(self):
        size_in_bytes = sum(sys.getsizeof(self.memTable) + sys.getsizeof(v) for k,v in self.memTable.items())
        size_in_mb = size_in_bytes / 1024 / 1024
        if size_in_mb > 1:
            prevId = self.sstable_manager.retrieve_last_id_from_id_file()
            if prevId is None:
                prevId = "ID100"
            id = "ID" + str(int(prevId[2:]) + 1)
            sstable_new = SortedSetTable(SSTableId=id, nextSSTableId=prevId, data=self.memTable, timestamp=str(datetime.now()))
            self.memTable = {}
            self.sstable_manager.write_sst(sstable_new)
            self.sstable_manager.write_new_id_in_id_file(id)
        else:
            return

    def __init__(self):
        # Managers
        self.memTableManager = memTableManager()
        self.sstable_manager = SSTableManager()

        # Mem Table Reconstruction
        self.wals : Dict =  memTableManager.reconstructMemTable(self.memTableManager)
        self.memTable = {}
        for key in self.wals.keys():
            if key[0] == "w":
                data = self.wals[key]
                self.memTable[data["key"]] = data["value"]

    async def write_memTable(self, key, value) -> None:
        keys = list(self.memTable.keys())
        keys.append(key)
        keys.sort()
        new_memTable = {}
        for i in keys:
            if i == key:
                new_memTable[i] = value
            else:
                new_memTable[i] = self.memTable[i]
        self.memTable = new_memTable
        await self.dump_memtable_to_sstable()

    async def read_memTable(self, key):
        if(key in self.memTable):
            return self.memTable[key]
        else:
            ss_tables = self.sstable_manager.read_sstables(self.sstable_manager.retrieve_last_id_from_id_file())
            reversed_ss_tables = list(reversed(ss_tables))
            for reversed_ss_table in reversed_ss_tables:
                if reversed_ss_table.data.get(key) is not None:
                    return reversed_ss_table.data[key]