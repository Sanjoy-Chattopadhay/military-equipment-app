import pandas as pd
import streamlit as st
from sqlalchemy import text
from config import get_connection

class DatabaseQueries:
    def __init__(self):
        self.engine = get_connection()

    def get_subcategories(self):
        """Get subcategory list for category B"""
        query = """
            SELECT subcategoryname 
            FROM tsubcat 
            WHERE categoryname = 'B' 
            AND subcategoryname IS NOT NULL
            ORDER BY subcategoryname
        """
        df = pd.read_sql(query, self.engine)
        subcats = df["subcategoryname"].dropna().unique().tolist()
        return ["All"] + subcats

    def get_subcatid(self, subcategory_name):
        """Get SubCatID from name"""
        query = """
            SELECT subcatid 
            FROM tsubcat
            WHERE categoryname = 'B' 
            AND subcategoryname = %s
        """
        df = pd.read_sql(query, self.engine, params=[subcategory_name])
        return df['subcatid'].iloc[0] if not df.empty else None

    def get_user_units(self):
        """Get active user units"""
        query = """
            SELECT userunit_id, userunit_name
            FROM tuserunit 
            WHERE movedout = false
            AND userunit_name IS NOT NULL
            ORDER BY userunit_name
        """
        df = pd.read_sql(query, self.engine)
        df = df.dropna(subset=["userunit_id", "userunit_name"])
        return [("All", None)] + list(df[["userunit_name", "userunit_id"]].itertuples(index=False, name=None))

    def get_eqpt_records(self, subcat_id=None, user_unit_id=None, selected_year=None):
        """Get equipment records with filters"""
        query = """
            SELECT r.id AS eqptid, r.regnno, r.nomenclature, r.dtofissue, r.inkm, u.userunit_name
            FROM teqptrecord r
            LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
            LEFT JOIN tsubcat s ON r.cat = s.subcatid
            WHERE s.categoryname = 'B'
        """
        params = []

        if subcat_id is not None:
            query += " AND r.cat = %s"
            params.append(int(subcat_id))

        if user_unit_id is not None:
            query += " AND r.userunit = %s"
            params.append(int(user_unit_id))

        if selected_year and selected_year != "All":
            query += " AND EXTRACT(YEAR FROM r.dtofissue) >= %s"
            params.append(int(selected_year))

        query += " ORDER BY r.regnno"

        return pd.read_sql(query, self.engine, params=params)

    def get_fault_counts_per_eqpt(self):
        """Fetch total fault count per EqptID"""
        query = """
            SELECT r.id AS eqptid
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE f.faults IS NOT NULL
        """
        df = pd.read_sql(query, self.engine)
        return (
            df.groupby('eqptid')
            .size()
            .reset_index(name='totalfaultcount')
        )

    def get_critical_fault_counts_per_eqpt(self, subcat_id=None, user_unit_id=None, selected_year=None):
        """Get critical fault counts per equipment"""
        try:
            query = """
                SELECT r.id AS eqptid
                FROM teqptrecord r
                LEFT JOIN jobcard jc ON r.id = jc.referid
                LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
                LEFT JOIN tfaults f ON jcd.fault = f.faultid
                LEFT JOIN tsubcat s ON r.cat = s.subcatid
                LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
                WHERE jcd.critical = 1 AND s.categoryname = 'B'
            """
            params = []

            if subcat_id is not None:
                query += " AND r.cat = %s"
                params.append(int(subcat_id))

            if user_unit_id is not None:
                query += " AND r.userunit = %s"
                params.append(int(user_unit_id))

            if selected_year and selected_year != "All":
                query += " AND EXTRACT(YEAR FROM jc.jobcarddate) >= %s"
                params.append(int(selected_year))

            df = pd.read_sql(query, self.engine, params=params)

            if df.empty:
                return pd.DataFrame(columns=['eqptid', 'totalcriticalfaultcount'])

            return (
                df.groupby('eqptid')
                .size()
                .reset_index(name='totalcriticalfaultcount')
            )

        except Exception as e:
            st.error(f"Error in get_critical_fault_counts_per_eqpt: {e}")
            return pd.DataFrame(columns=['eqptid', 'totalcriticalfaultcount'])

    def get_fault_counts_and_descriptions_per_eqpt(self):
        """Fetch total fault count and descriptions per EqptID"""
        query = """
            SELECT r.id AS eqptid, f.faults AS faultdescription
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE f.faults IS NOT NULL
        """
        df = pd.read_sql(query, self.engine)

        # Group by equipment and aggregate
        result = df.groupby('eqptid').agg(
            totalfaultcount=('faultdescription', 'count'),
            allfaults=('faultdescription', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()

        return result

    def get_detailed_fault_info(self, eqpt_id):
        """Get detailed fault information for specific equipment"""
        query = """
            SELECT r.id AS eqptid, f.faults AS faultdescription
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE r.id = %s AND f.faults IS NOT NULL
        """
        return pd.read_sql(query, self.engine, params=[eqpt_id])

    def get_equipment_details(self, regn_no, selected_year=None):
        """Get detailed equipment history"""
        try:
            query = """
                SELECT e.regnno, e.nomenclature, j.jobcardno, j.jobcarddate,
                       f.faults, sm.itemname, tr.issues
                FROM teqptrecord e
                LEFT JOIN jobcard j ON e.id = j.referid
                LEFT JOIN jobcarddetails jd ON j.id = jd.refjobno
                LEFT JOIN tfaults f ON jd.fault = f.faultid
                LEFT JOIN tsstransactionregister tr ON j.id = tr.refjobid
                LEFT JOIN tssstockmaster sm ON tr.partnoid = sm.id
                WHERE e.regnno = %s
                AND (tr.issues > 0 OR tr.issues IS NULL)
            """
            params = [regn_no]

            if selected_year and selected_year != "All":
                query += " AND EXTRACT(YEAR FROM j.jobcarddate) >= %s"
                params.append(int(selected_year))

            query += " ORDER BY j.jobcarddate DESC"

            df = pd.read_sql(query, self.engine, params=params)

            if df.empty:
                return None, None, df

            # Get header info
            regn = df['regnno'].iloc[0]
            nomen = df['nomenclature'].iloc[0]

            # Group by JobCard and aggregate faults/spares
            jobcard_groups = df.groupby(['jobcardno', 'jobcarddate']).agg({
                'faults': lambda x: '; '.join([str(fault) for fault in x.dropna().unique() if str(fault) != 'nan']),
                'itemname': lambda x: '; '.join([str(item) for item in x.dropna().unique() if str(item) != 'nan']),
                'issues': lambda x: x.sum() if x.notna().any() else None
            }).reset_index()

            # Clean up empty aggregations
            jobcard_groups['faults'] = jobcard_groups['faults'].replace('', None)
            jobcard_groups['itemname'] = jobcard_groups['itemname'].replace('', None)
            jobcard_groups['faults'] = jobcard_groups['faults'].fillna('No faults recorded')
            jobcard_groups['itemname'] = jobcard_groups['itemname'].fillna('No spares used')

            return regn, nomen, jobcard_groups

        except Exception as e:
            st.error(f"Database error: {e}")
            return None, None, pd.DataFrame()
