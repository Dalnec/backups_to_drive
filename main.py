import uvicorn
import os
import httpx
from datetime import datetime

from fastapi import FastAPI, APIRouter, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse
from sqlmodel import Session, select
from typing import Annotated
from contextlib import asynccontextmanager
from config.db import create_db_and_tables, get_session
from sqlalchemy import event
from typing import Union, List
from models.user import User
from models.database import Database
from models.server import Server
from schemas.user import UserCreate, UserPublic, UserUpdate
from schemas.databases import DatabaseCreate, DatabasePublic, DatabaseUpdate, DatabaseInfo
from schemas.server import ServerCreate, ServerPublic, ServerUpdate
from drive.send_drive import searchFile
from db import create_file, get_databases


app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código para el inicio (startup)
    create_db_and_tables()
    yield  # Aquí la app está corriendo
    # Código para el cierre (shutdown)
    print("Aplicación terminada")

SessionDep = Annotated[Session, Depends(get_session)]
app = FastAPI(lifespan=lifespan)

# Configuración de CORS
origins = [
    "http://localhost",
    "http://localhost:5173",  # Para permitir React/Vue/Angular en desarrollo
]
app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Permitir estos orígenes
    allow_credentials=True,  # Permitir el uso de cookies
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permitir todos los encabezados
)

api_router = APIRouter(prefix="/api")

@app.get("/")
def read_root():
    # return {"message": "Ola q ace"}
    return FileResponse("static/index.html")
@app.get("/servers")
def read_root():
    # return {"message": "Ola q ace"}
    return FileResponse("static/index.html")

@api_router.get("/database/list", response_model=List[DatabaseInfo])
async def get_all_databases():
    raw_data = get_databases()
    result = [{"datname": db, "rolname": role} for db, role in raw_data]
    return result

@api_router.post("/database/create/backup")
async def create_backup(db: DatabaseInfo):
    file_name, gz_file = create_file(db.datname, db.rolname)
    os.remove(file_name)
    file = searchFile(gz_file, gz_file, "application/gzip")
    os.remove(gz_file)
    return {"success": True, "id_drive": 0, "name": db.datname}

@api_router.post("/database", tags=["database"], response_model=DatabasePublic)
async def create_database(database: DatabaseCreate, session: SessionDep):
    data = Database.model_validate(database)
    session.add(data)
    session.commit()
    session.refresh(data)
    return data

@api_router.get("/database", tags=["database"], response_model=list[DatabasePublic])
async def read_databases(session: SessionDep):
    databases = session.exec(select(Database)).all()
    return databases

@api_router.patch("/database/{database_id}", tags=["database"], response_model=DatabasePublic)
def update_database(database_id: int, database: DatabaseUpdate, session: SessionDep):
    database_db = session.get(User, database_id)
    if not database_db:
        raise HTTPException(status_code=404, detail="database not found")
    database_data = database.model_dump(exclude_unset=True)
    database_db.sqlmodel_update(database_data)
    session.add(database_db)
    session.commit()
    session.refresh(database_db)
    return database_db


@api_router.delete("/database/{database_id}", tags=["database"])
def delete_database(database_id: int, session: SessionDep):
    database = session.get(database, database_id)
    if not database:
        raise HTTPException(status_code=404, detail="database not found")
    session.delete(database)
    session.commit()
    return {"success": True}

@api_router.post("/user", tags=["user"], response_model=UserPublic)
async def create_user(user: UserCreate, session: SessionDep):
    data = User.model_validate(user)
    session.add(data)
    session.commit()
    session.refresh(data)
    return data

@api_router.get("/user", tags=["user"], response_model=list[UserPublic])
async def read_users(session: SessionDep):
    users = session.exec(select(User)).all()
    return users

@api_router.patch("/user/{user_id}", tags=["user"], response_model=UserPublic)
def update_user(user_id: int, user: UserUpdate, session: SessionDep):
    user_db = session.get(User, user_id)
    if not user_db:
        raise HTTPException(status_code=404, detail="User not found")
    user_data = user.model_dump(exclude_unset=True)
    user_db.sqlmodel_update(user_data)
    session.add(user_db)
    session.commit()
    session.refresh(user_db)
    return user_db


@api_router.delete("/user/{user_id}", tags=["user"])
def delete_user(user_id: int, session: SessionDep):
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    session.delete(user)
    session.commit()
    return {"success": True}

# Evento para actualizar automáticamente el campo `updated`
@event.listens_for(Server, "before_update")
def set_updated_timestamp(mapper, connection, target):
    target.updated = datetime.now(datetime.timezone.utc)

@api_router.post("/server", tags=["server"], response_model=ServerPublic)
async def create_server(server: ServerCreate, session: SessionDep):
    # data = server.model_validate(server)
    data = Server(name=server.name, url=server.url)
    session.add(data)
    session.commit()
    session.refresh(data)
    return data

@api_router.get("/server", tags=["server"], response_model=list[ServerPublic])
async def read_servers(session: SessionDep):
    servers = session.exec(select(Server)).all()
    return servers

@api_router.patch("/server/{server_id}", tags=["server"], response_model=ServerPublic)
def update_server(server_id: int, server: ServerUpdate, session: SessionDep):
    server_db = session.get(server, server_id)
    if not server_db:
        raise HTTPException(status_code=404, detail="server not found")
    server_data = server.model_dump(exclude_unset=True)
    server_db.sqlmodel_update(server_data)
    session.add(server_db)
    session.commit()
    session.refresh(server_db)
    return server_db


@api_router.delete("/server/{server_id}", tags=["server"])
def delete_server(server_id: int, session: SessionDep):
    server = session.get(server, server_id)
    if not server:
        raise HTTPException(status_code=404, detail="server not found")
    session.delete(server)
    session.commit()
    return {"success": True}

@api_router.get("/server/db/list/{server_id}", tags=["server"])
async def list_backup(server_id: int, session: SessionDep):
    server = session.exec(select(Server).filter(Server.id == server_id)).first()
    url = f'{server.url}/api/database/list'
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        data = response.json()
        print(data)
    return data

@api_router.put("/server/db/create/{server_id}", tags=["server"])
async def create_backup(server_id: int, db: dict, session: SessionDep):
    # Buscar el servidor en la base de datos
    server = session.exec(select(Server).filter(Server.id == server_id)).first()
    if not server:
        return {"error": "Server not found"}

    # Construir la URL de la API externa
    url = f"{server.url}/api/database/create/backup"

    # Consumir la API externa de forma asíncrona
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(url, json=db)  # Usa `json` para datos estructurados
            response.raise_for_status()  # Lanza excepción si el código no es 2xx
            data = response.json()  # Convertir la respuesta en JSON
            return data
        except httpx.HTTPStatusError as e:
            return {"error": f"Error en la API externa: {str(e)}"}
        except httpx.RequestError as e:
            return {"error": f"Error de red: {str(e)}"}

app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=5000, reload=True)