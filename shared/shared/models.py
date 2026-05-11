from pydantic import BaseModel, Field
from typing import Any, Dict


class JobRequest(BaseModel):
    job_id: str = Field(..., min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)


class JobResult(BaseModel):
    job_id: str
    status: str
    output: Dict[str, Any] = Field(default_factory=dict)