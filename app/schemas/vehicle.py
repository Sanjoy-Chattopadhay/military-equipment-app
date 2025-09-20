"""Pydantic schemas for vehicle-related data"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class VehicleBase(BaseModel):
    regnno: str
    nomenclature: str
    dtofissue: Optional[datetime] = None
    inkm: Optional[int] = 0

class VehicleResponse(VehicleBase):
    eqptid: int
    userunit_name: Optional[str] = None
    year: Optional[int] = None
    total_fault_count: int = 0
    total_critical_fault_count: int = 0
    respect_to_vintage: str = "Unknown"
    respect_to_distance: str = "Unknown"
    respect_to_critical_faults: str = "Unknown"
    priority: str = "P5"

class PaginatedVehicleResponse(BaseModel):
    vehicles: List[VehicleResponse]
    total_count: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_previous: bool

class SubcategoryResponse(BaseModel):
    subcategoryname: str

class UserUnitResponse(BaseModel):
    userunit_id: int
    userunit_name: str

class FaultHistoryResponse(BaseModel):
    jobcardno: Optional[str] = None
    jobcarddate: Optional[datetime] = None
    faults: Optional[str] = None
    itemname: Optional[str] = None
    issues: Optional[int] = None

class PaginatedHistoryResponse(BaseModel):
    history: List[FaultHistoryResponse]
    total_count: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_previous: bool

class MaintenanceTaskResponse(BaseModel):
    message: str
    tasks: List[str] = []

class VehicleFilters(BaseModel):
    subcat_id: Optional[int] = None
    user_unit_id: Optional[int] = None
    selected_year: Optional[int] = None
    page: int = 1
    page_size: int = 20
