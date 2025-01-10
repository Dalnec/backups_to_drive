from sqlmodel import Field, SQLModel
from datetime import datetime, timezone, timedelta

# Configura la zona horaria local manualmente (por ejemplo, UTC-5)
LOCAL_TIMEZONE = timezone(timedelta(hours=-5))
def get_local_time():
    """Devuelve la hora local formateada como yyyy-MM-dd hh:mm:ss."""
    return datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

class ServerBase(SQLModel):
    name: str = Field(index=True)
    url: str
    # created: str
    # updated: str
    created: str = Field(default_factory=get_local_time, nullable=False)
    updated: str = Field(default_factory=get_local_time, nullable=False)
    
class Server(ServerBase, table=True):
    id: int | None = Field(default=None, primary_key=True)