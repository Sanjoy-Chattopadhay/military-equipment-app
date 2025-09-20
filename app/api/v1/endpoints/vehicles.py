"""Vehicle endpoints"""
from fastapi import APIRouter, Depends, Query
from typing import List, Optional
from app.schemas.vehicle import *
from app.services.vehicle_service import vehicle_service

router = APIRouter()

@router.get("/subcategories", response_model=List[SubcategoryResponse])
async def get_subcategories():
    """Get all vehicle subcategories"""
    return vehicle_service.get_subcategories()

@router.get("/user-units", response_model=List[UserUnitResponse])
async def get_user_units():
    """Get all active user units"""
    return vehicle_service.get_user_units()

@router.get("/vehicles", response_model=PaginatedVehicleResponse)
async def get_vehicles_paginated(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    subcat_id: Optional[int] = Query(None),
    user_unit_id: Optional[int] = Query(None),
    selected_year: Optional[int] = Query(None)
):
    """Get vehicles with applied filters and pagination"""
    filters = VehicleFilters(
        subcat_id=subcat_id,
        user_unit_id=user_unit_id,
        selected_year=selected_year,
        page=page,
        page_size=page_size
    )
    return vehicle_service.get_vehicles_paginated(filters)

@router.get("/vehicles/{regn_no}/history", response_model=PaginatedHistoryResponse)
async def get_vehicle_history_paginated(
    regn_no: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    selected_year: Optional[int] = Query(None)
):
    """Get paginated vehicle history"""
    return vehicle_service.get_vehicle_history_paginated(
        regn_no, selected_year, page, page_size
    )

@router.get("/vehicles/maintenance-forecast", response_model=MaintenanceTaskResponse)
async def get_maintenance_forecast(
    current_km: int = Query(...),
    input_km: int = Query(...)
):
    """Get maintenance forecast for given km range"""
    return vehicle_service.get_maintenance_forecast(current_km, input_km)

@router.get("/subcategories/{subcategory_name}/id")
async def get_subcategory_id(subcategory_name: str):
    """Get subcategory ID by name"""
    subcat_id = vehicle_service.get_subcatid(subcategory_name)
    return {"subcategoryname": subcategory_name, "subcatid": subcat_id}
