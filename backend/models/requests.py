from pydantic import BaseModel
from typing import Optional


class CompanyProfileRequest(BaseModel):
    name:               str
    url:                Optional[str]   = None
    description_input:  Optional[str]   = None
    industry:           Optional[str]   = None
    customer_type:      Optional[str]   = None  # B2C | B2B | mixed


class ObjectiveRequest(BaseModel):
    id:         str     = "OBJ-01"
    statement:  str
    verb:       Optional[str] = None
    object:     Optional[str] = None


class SOBJRequest(BaseModel):
    id:         str
    statement:  str
    direction:  str     # increase | decrease | maintain | initiate | stop


class SOBJStatusUpdate(BaseModel):
    status:         str             # approved | amend_requested
    amendment_note: Optional[str]   = None
