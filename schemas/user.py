from models.user import User, UserBase

class UserCreate(UserBase):
    pass

class UserPublic(UserBase):
    id: int

class UserUpdate(UserBase):
    email: str | None = None
    password: str | None = None