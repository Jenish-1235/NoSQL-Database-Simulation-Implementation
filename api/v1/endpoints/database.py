from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def root():
    return {"message": "Welcome to NoSQL Database"}


@router.get("/write")
async def write():
    return {"message": "Writing to database"}

@router.get("/read")
async def read():
    return {"message": "Reading from database"}