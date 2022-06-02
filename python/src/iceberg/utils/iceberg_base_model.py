from pydantic import BaseModel


class IcebergBaseModel(BaseModel):
    class Config:
        allow_population_by_field_name = True

    def dict(self, exclude_none=True, **kwargs):
        return super().dict(exclude_none=exclude_none, **kwargs)

    def json(self, exclude_none=True, by_alias=True, **kwargs):
        return super().json(exclude_none=exclude_none, by_alias=True, **kwargs)
