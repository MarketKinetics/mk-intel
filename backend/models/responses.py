from pydantic import BaseModel
from typing import Optional


class SessionResponse(BaseModel):
    session_id:     str
    status:         str
    session_mode:   str
    company_name:   Optional[str]   = None
    obj_statement:  Optional[str]   = None
    sobj_count:     int             = 0
    sobjs_approved: int             = 0
    created_at:     str
    updated_at:     str


class JobStatusResponse(BaseModel):
    job_id:         str
    session_id:     str
    job_type:       str
    status:         str
    progress:       Optional[str]   = None
    started_at:     Optional[str]   = None
    completed_at:   Optional[str]   = None
    error:          Optional[str]   = None


class SOBJResponse(BaseModel):
    id:             str
    statement:      str
    direction:      str
    status:         str
    version:        int
