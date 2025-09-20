"""API v1 router"""
from fastapi import APIRouter
from app.api.v1.endpoints import vehicles

api_router = APIRouter()
api_router.include_router(vehicles.router, prefix="/vehicles", tags=["vehicles"])
