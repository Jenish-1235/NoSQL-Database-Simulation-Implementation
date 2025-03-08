from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from api.v1.api import api_router
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

app.include_router(api_router, prefix="/api")
