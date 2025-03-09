from typing import Any, Dict

from fastapi import APIRouter
from fastapi.params import Depends
import schemas.data
from core.WAL_manager import WALManager

from services.database_service import DatabaseService

router = APIRouter()

database_service = DatabaseService()
wal_manager = WALManager()

def get_database_service():
    return database_service

def get_wal_manager():
    return wal_manager

@router.get("/")
async def root():
    return {"message": "Welcome to NoSQL Database"}


@router.post("/write")
async def write(data: Dict[str, Any], database_service:DatabaseService = Depends(get_database_service), wal_manager : WALManager = Depends(get_wal_manager)):
    try:
        key = data["key"]
        value = data["value"]
        await database_service.write_memTable(key, value)
        await wal_manager.update_wal_file(wal_data={"key": key, "value": value} , operation="w")
        return {"message": "Success"}
    except Exception as e:
        return {"message": str(e)}

@router.get("/read")
async def read(data: Dict[str, str], database_service:DatabaseService = Depends(get_database_service)):
    try:
        key = data["key"]
        value = await database_service.read_memTable(key)
        data = {"key": key, "value": value}
        return data
    except Exception as e:
        return {"message": str(e)}