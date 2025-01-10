from sqlmodel import Field, SQLModel

class DatabaseBase(SQLModel):
    name: str = Field(index=True)
    owner: str
    drive_name: str = Field(index=True)
    drive_id: str
    created: str
    updated: str

class Database(DatabaseBase, table=True):
    id: int | None = Field(default=None, primary_key=True)