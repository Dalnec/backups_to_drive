from models.database import Database, DatabaseBase
from pydantic import BaseModel
from typing import List

class DatabaseInfo(BaseModel):
    datname: str
    rolname: str


class DatabaseCreate(DatabaseBase):
    pass

class DatabasePublic(DatabaseBase):
    id: int

class DatabaseUpdate(DatabaseBase):
    name: str | None = None
    owner: str | None = None
    drive_name: str | None = None
    drive_id: str | None = None
    created: str | None = None
    updated: str | None = None

class SuccessDatabaseResponse(BaseModel):
    success: bool
    data: DatabasePublic