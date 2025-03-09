"""
Class responsible as service for handling all the NoSQL CRUD Tasks...
"""
from typing import Dict


class DatabaseService:
    def __init__(self):
        self.memTable : Dict =  {}

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

    async def read_memTable(self, key):
        return self.memTable[key]