from models.server import Server, ServerBase
from pydantic import BaseModel

# class ServerCreate(ServerBase):
#     pass
class ServerCreate(BaseModel):
    name: str
    url: str

class ServerPublic(ServerBase):
    id: int

class ServerUpdate(ServerBase):
    name: str | None = None
    url: str | None = None
    created: str | None = None
    updated: str | None = None

class SuccessServerResponse(BaseModel):
    success: bool
    data: ServerPublic