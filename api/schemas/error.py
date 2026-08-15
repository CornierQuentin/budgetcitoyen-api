"""Schema d'erreur au format RFC 7807 (application/problem+json)."""

from pydantic import BaseModel, ConfigDict


class ProblemDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    type: str = "about:blank"
    title: str
    status: int
    detail: str
    instance: str | None = None
