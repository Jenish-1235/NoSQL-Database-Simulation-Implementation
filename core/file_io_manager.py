"""
Abstraction to quickly handle file I/O throughout the database service
"""

class FileIOManager:
    def __init__(self):
        self.wal_file = None
        self.ssTable_file = None
        self.lookup_map_file = None

    def openWAL(self, wal_file):
        self.wal_file = wal_file
        wal_file = open(wal_file, "r+")
        return wal_file

    def writeWAL(self, wal_file, wal_record):
        self.wal_file = wal_file
        wal_file = open(wal_file, "a+")
        wal_file.write(wal_record)
        wal_file.close()

    def closeWAL(self):
        if self.wal_file is not None:
            self.wal_file.close()
            self.wal_file = None
        else:
            print("No wal file opened")

    def openSS(self, ssTable_file):
        self.ssTable_file = ssTable_file
        ssTable_file = open(ssTable_file, "a+")

    def closeSS(self):
        if self.ssTable_file is not None:
            self.ssTable_file.close()
            self.ssTable_file = None
        else:
            print("No ssTable file opened")

    def openLookupMap(self, lookup_map_file):
        self.lookup_map_file = lookup_map_file
        lookup_map_file = open(lookup_map_file, "a+")

    def closeLookupMap(self):
        if self.lookup_map_file is not None:
            self.lookup_map_file.close()
            self.lookup_map_file = None
        else:
            print("No lookupMap file opened")
