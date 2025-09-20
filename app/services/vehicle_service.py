"""Optimized business logic for vehicle-related operations"""
from app.db.database import db
from app.schemas.vehicle import *
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
from datetime import datetime
import math
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VehicleService:

    def __init__(self):
        # Cache for subcategories and user units (they don't change frequently)
        self._subcategories_cache = None
        self._user_units_cache = None

    def get_subcategories(self) -> List[SubcategoryResponse]:
        """Get all subcategories for category 'B' with caching"""
        if self._subcategories_cache is not None:
            return self._subcategories_cache

        query = "SELECT DISTINCT subcategoryname FROM tsubcat WHERE categoryname = 'B' ORDER BY subcategoryname"
        df = db.execute_query(query)
        subcats = df["subcategoryname"].dropna().unique().tolist()
        self._subcategories_cache = [SubcategoryResponse(subcategoryname=subcat) for subcat in subcats]
        return self._subcategories_cache

    def get_user_units(self) -> List[UserUnitResponse]:
        """Get all active user units with caching"""
        if self._user_units_cache is not None:
            return self._user_units_cache

        query = """
            SELECT userunit_id, userunit_name 
            FROM tuserunit 
            WHERE movedout = FALSE 
            ORDER BY userunit_name
        """
        df = db.execute_query(query)
        df = df.dropna(subset=["userunit_id", "userunit_name"])
        self._user_units_cache = [UserUnitResponse(**row) for _, row in df.iterrows()]
        return self._user_units_cache

    def get_vehicles_paginated(self, filters: VehicleFilters) -> PaginatedVehicleResponse:
        """Get vehicles with optimized queries and pagination"""
        logger.info(f"Fetching vehicles: page={filters.page}, filters={filters}")

        try:
            # Build WHERE conditions
            where_conditions = ["s.categoryname = 'B'"]
            params = []

            if filters.subcat_id is not None:
                where_conditions.append("r.cat = %s")
                params.append(int(filters.subcat_id))

            if filters.user_unit_id is not None:
                where_conditions.append("r.userunit = %s")
                params.append(int(filters.user_unit_id))

            where_clause = " AND ".join(where_conditions)

            # Optimized count query
            count_query = f"""
                SELECT COUNT(*) as total_count
                FROM teqptrecord r
                INNER JOIN tsubcat s ON r.cat = s.subcatid
                WHERE {where_clause}
            """

            # Get total count
            count_result = db.execute_query(count_query, params=params)
            total_count = count_result['total_count'].iloc[0] if not count_result.empty else 0

            if total_count == 0:
                return PaginatedVehicleResponse(
                    vehicles=[],
                    total_count=0,
                    page=filters.page,
                    page_size=filters.page_size,
                    total_pages=0,
                    has_next=False,
                    has_previous=False
                )

            # Calculate pagination
            total_pages = math.ceil(total_count / filters.page_size)
            offset = (filters.page - 1) * filters.page_size

            # Optimized main query with minimal joins
            main_query = f"""
                SELECT 
                    r.id AS eqptid, 
                    r.regnno, 
                    r.nomenclature, 
                    r.dtofissue, 
                    r.inkm, 
                    u.userunit_name
                FROM teqptrecord r
                INNER JOIN tsubcat s ON r.cat = s.subcatid
                LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
                WHERE {where_clause}
                ORDER BY r.id
                LIMIT {filters.page_size} OFFSET {offset}
            """

            df = db.execute_query(main_query, params=params)

            if df.empty:
                return PaginatedVehicleResponse(
                    vehicles=[],
                    total_count=total_count,
                    page=filters.page,
                    page_size=filters.page_size,
                    total_pages=total_pages,
                    has_next=False,
                    has_previous=False
                )

            # Process data efficiently
            df['year'] = pd.to_datetime(df['dtofissue'], errors='coerce').dt.year
            df['inkm'] = pd.to_numeric(df['inkm'], errors='coerce').fillna(0).astype(int)

            # Get vehicle IDs for fault queries
            vehicle_ids = df['eqptid'].tolist()

            # Get fault counts in batch
            fault_counts_df = self._get_fault_counts_batch(vehicle_ids)
            critical_fault_counts_df = self._get_critical_fault_counts_batch(vehicle_ids, filters)

            # Merge fault data efficiently
            df = df.merge(fault_counts_df, how='left', on='eqptid')
            df = df.merge(critical_fault_counts_df, how='left', on='eqptid')

            # Fill NaN values
            df['total_fault_count'] = df['total_fault_count'].fillna(0).astype(int)
            df['total_critical_fault_count'] = df['total_critical_fault_count'].fillna(0).astype(int)

            # Calculate metrics efficiently
            df['respect_to_vintage'] = df['year'].apply(self._calculate_vintage_respect_vectorized)
            df['respect_to_distance'] = df['inkm'].apply(self._calculate_km_respect_vectorized)
            df['respect_to_critical_faults'] = df['total_critical_fault_count'].apply(self._calculate_critical_fault_respect_vectorized)

            # Calculate priority
            df['priority'] = df.apply(self._calculate_priority_vectorized, axis=1)

            vehicles = [VehicleResponse(**row) for _, row in df.iterrows()]

            return PaginatedVehicleResponse(
                vehicles=vehicles,
                total_count=total_count,
                page=filters.page,
                page_size=filters.page_size,
                total_pages=total_pages,
                has_next=filters.page < total_pages,
                has_previous=filters.page > 1
            )

        except Exception as e:
            logger.error(f"Error in get_vehicles_paginated: {e}")
            raise e

    def _get_fault_counts_batch(self, vehicle_ids: List[int]) -> pd.DataFrame:
        """Get fault counts for multiple vehicles in one optimized query"""
        if not vehicle_ids:
            return pd.DataFrame(columns=['eqptid', 'total_fault_count'])

        placeholders = ','.join(map(str, vehicle_ids))
        query = f"""
            SELECT 
                r.id AS eqptid, 
                COALESCE(COUNT(f.faultid), 0) as total_fault_count
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE r.id IN ({placeholders})
            GROUP BY r.id
        """

        try:
            return db.execute_query(query)
        except Exception as e:
            logger.error(f"Error in fault counts batch: {e}")
            return pd.DataFrame({'eqptid': vehicle_ids, 'total_fault_count': [0] * len(vehicle_ids)})

    def _get_critical_fault_counts_batch(self, vehicle_ids: List[int], filters: VehicleFilters) -> pd.DataFrame:
        """Get critical fault counts for multiple vehicles in one optimized query"""
        if not vehicle_ids:
            return pd.DataFrame(columns=['eqptid', 'total_critical_fault_count'])

        placeholders = ','.join(map(str, vehicle_ids))
        query = f"""
            SELECT 
                r.id AS eqptid, 
                COALESCE(COUNT(CASE WHEN jcd.critical = 1 THEN 1 END), 0) as total_critical_fault_count
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            WHERE r.id IN ({placeholders})
        """

        # Add optional filters
        params = []
        if filters.selected_year and filters.selected_year != "All":
            query += " AND EXTRACT(YEAR FROM jc.jobcarddate) >= %s"
            params.append(int(filters.selected_year))

        query += " GROUP BY r.id"

        try:
            return db.execute_query(query, params=params)
        except Exception as e:
            logger.error(f"Error in critical fault counts batch: {e}")
            return pd.DataFrame({'eqptid': vehicle_ids, 'total_critical_fault_count': [0] * len(vehicle_ids)})

    def get_vehicle_history_paginated(
            self,
            regn_no: str,
            selected_year: Optional[int] = None,
            page: int = 1,
            page_size: int = 20
    ) -> PaginatedHistoryResponse:
        """Get paginated vehicle history"""
        # Count query
        count_query = """
            SELECT COUNT(DISTINCT j.id) as total_count
            FROM teqptrecord e
                LEFT JOIN jobcard j ON e.id = j.referid
                LEFT JOIN jobcarddetails jd ON j.id = jd.refjobno
                LEFT JOIN tfaults f ON jd.fault = f.faultid
                LEFT JOIN tsstransactionregister tr ON j.id = tr.refjobid
                LEFT JOIN tssstockmaster sm ON tr.partnoid = sm.id
            WHERE e.regnno = %s AND (tr.issues > 0 OR tr.issues IS NULL)
        """

        params = [regn_no]

        if selected_year and selected_year != "All":
            count_query += " AND EXTRACT(YEAR FROM j.jobcarddate) >= %s"
            params.append(int(selected_year))

        # Get total count
        count_result = db.execute_query(count_query, params=params)
        total_count = count_result['total_count'].iloc[0] if not count_result.empty else 0

        # Calculate pagination
        total_pages = math.ceil(total_count / page_size)
        offset = (page - 1) * page_size

        # Main query with pagination
        query = """
            SELECT e.regnno, e.nomenclature, j.jobcardno, j.jobcarddate, f.faults, 
                   sm.itemname, tr.issues
            FROM teqptrecord e
                LEFT JOIN jobcard j ON e.id = j.referid
                LEFT JOIN jobcarddetails jd ON j.id = jd.refjobno
                LEFT JOIN tfaults f ON jd.fault = f.faultid
                LEFT JOIN tsstransactionregister tr ON j.id = tr.refjobid
                LEFT JOIN tssstockmaster sm ON tr.partnoid = sm.id
            WHERE e.regnno = %s AND (tr.issues > 0 OR tr.issues IS NULL)
        """

        if selected_year and selected_year != "All":
            query += " AND EXTRACT(YEAR FROM j.jobcarddate) >= %s"

        query += f" ORDER BY j.jobcarddate DESC LIMIT {page_size} OFFSET {offset}"

        df = db.execute_query(query, params=params)

        if df.empty:
            return PaginatedHistoryResponse(
                history=[],
                total_count=total_count,
                page=page,
                page_size=page_size,
                total_pages=total_pages,
                has_next=False,
                has_previous=False
            )

        # Group by JobCard to combine faults and spares
        grouped = df.groupby(['jobcardno', 'jobcarddate']).agg({
            'faults': lambda x: '; '.join([str(f) for f in x.dropna().unique() if str(f) != 'nan']),
            'itemname': lambda x: '; '.join([str(i) for i in x.dropna().unique() if str(i) != 'nan']),
            'issues': lambda x: x.sum() if x.notna().any() else None
        }).reset_index()

        # Clean up data
        grouped['faults'] = grouped['faults'].replace('', 'No faults recorded')
        grouped['itemname'] = grouped['itemname'].replace('', 'No spares used')

        history = [FaultHistoryResponse(**row) for _, row in grouped.iterrows()]

        return PaginatedHistoryResponse(
            history=history,
            total_count=total_count,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )

    # Vectorized calculation methods for better performance
    def _calculate_vintage_respect_vectorized(self, year) -> str:
        """Vectorized vintage calculation"""
        if pd.isna(year):
            return 'Unknown'
        try:
            year = int(year)
            if year <= 2009:
                return 'Not Reliable'
            elif year < 2015:
                return 'Partially Reliable'
            return 'Reliable'
        except:
            return 'Invalid'

    def _calculate_km_respect_vectorized(self, km) -> str:
        """Vectorized km calculation"""
        if pd.isna(km):
            return 'Unknown'
        try:
            if km <= 40000:
                return 'Reliable'
            elif km <= 90000:
                return 'Partially Reliable'
            return 'Not Reliable'
        except:
            return 'Invalid'

    def _calculate_critical_fault_respect_vectorized(self, count) -> str:
        """Vectorized critical fault calculation"""
        if pd.isna(count):
            return 'Unknown'
        try:
            count = int(count)
            if count <= 2:
                return 'Reliable'
            elif 3 <= count <= 5:
                return 'Partially Reliable'
            else:
                return 'Not Reliable'
        except:
            return 'Invalid'

    def _calculate_priority_vectorized(self, row) -> str:
        """Vectorized priority calculation"""
        score_map = {
            'Reliable': 3,
            'Partially Reliable': 2,
            'Not Reliable': 1
        }

        cumulative_score = 0
        cumulative_score += score_map.get(row.get('respect_to_vintage'), 0)
        cumulative_score += score_map.get(row.get('respect_to_distance'), 0)
        cumulative_score += score_map.get(row.get('respect_to_critical_faults'), 0)

        if cumulative_score == 9:
            return 'P1'
        elif cumulative_score == 8:
            return 'P2'
        elif cumulative_score == 7:
            return 'P3'
        elif cumulative_score == 6:
            return 'P4'
        else:
            return 'P5'

    def get_maintenance_forecast(self, current_km: int, input_km: int) -> MaintenanceTaskResponse:
        """Calculate upcoming maintenance tasks - no change needed"""
        service_tasks = {
            5000: ["Change engine oil and oil filter", "Replace fuel filter", "Inspect and adjust brakes"],
            10000: ["Check gearbox and differential oil", "Inspect and adjust clutch", "Inspect suspension system"],
            20000: ["Engine tune-up", "Clean fuel tank and lines"]
        }

        if pd.isna(current_km) or pd.isna(input_km):
            return MaintenanceTaskResponse(message="Insufficient data to calculate upcoming maintenance.")

        try:
            future_km = current_km + input_km
            tasks = []

            for interval, task_list in service_tasks.items():
                next_due_km = ((current_km // interval) + 1) * interval
                km_remaining = next_due_km - current_km

                if next_due_km <= future_km:
                    task_info = f"After {km_remaining} km (at {next_due_km:,} km)"
                    tasks.extend([f"{task_info}: {task}" for task in task_list])

            if not tasks:
                return MaintenanceTaskResponse(
                    message=f"No scheduled maintenance within the next {input_km} km.",
                    tasks=[]
                )

            return MaintenanceTaskResponse(
                message=f"Maintenance due within next {input_km} km:",
                tasks=tasks
            )

        except Exception as e:
            return MaintenanceTaskResponse(message=f"Error calculating maintenance tasks: {e}")

    def get_subcatid(self, subcategory_name: str) -> Optional[int]:
        """Get subcategory ID by name"""
        query = """
            SELECT subcatid FROM tsubcat 
            WHERE categoryname = 'B' AND subcategoryname = %s
        """
        result = db.execute_query(query, params=[subcategory_name])
        return result['subcatid'].iloc[0] if not result.empty else None

vehicle_service = VehicleService()
