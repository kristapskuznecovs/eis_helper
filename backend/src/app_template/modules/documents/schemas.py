from pydantic import BaseModel


class DocumentRead(BaseModel):
    id: int
    filename: str
    storage_path: str
    status: str

    model_config = {"from_attributes": True}
