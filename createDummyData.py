import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date

# Neon DB connection
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ZgkW9VU8dBcY@ep-holy-cell-a7vl9851-pooler.ap-southeast-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


def clear_all_tables():
    """Clear all tables in proper order"""
    try:
        engine = create_engine(NEON_CONNECTION_STRING)

        # Order matters due to foreign key constraints
        tables_order = ['jobcarddetails', 'jobcard', 'teqptrecord', 'tfaults', 'tsubcat', 'tuserunit']

        with engine.connect() as conn:
            for table in tables_order:
                conn.execute(text(f'DELETE FROM "{table}"'))
                conn.commit()
                print(f"🧹 Cleared table: {table}")

    except Exception as e:
        print(f"❌ Clear operation failed: {e}")


def insert_sample_data():
    """Insert sample data into all tables with duplicate handling"""
    try:
        engine = create_engine(NEON_CONNECTION_STRING)

        print("🚀 Starting data insertion...")

        # 1. Insert into tUserUnit with ON CONFLICT handling
        print("[1/6] Inserting into tUserUnit...")

        with engine.connect() as conn:
            # Clear existing data first
            conn.execute(text('DELETE FROM "jobcarddetails"'))
            conn.execute(text('DELETE FROM "jobcard"'))
            conn.execute(text('DELETE FROM "teqptrecord"'))
            conn.execute(text('DELETE FROM "tfaults"'))
            conn.execute(text('DELETE FROM "tsubcat"'))
            conn.execute(text('DELETE FROM "tuserunit"'))
            conn.commit()
            print("🧹 Cleared all existing data")

            # Insert tUserUnit data
            tuserunit_query = text("""
                INSERT INTO tuserunit (userunit_id, userunit_name, userunit_address, userunit_remarks, uu_loc, password, movedout)
                VALUES (:userunit_id, :userunit_name, :userunit_address, :userunit_remarks, :uu_loc, :password, :movedout)
            """)

            tuserunit_data = [
                {"userunit_id": 1, "userunit_name": "1st Armored Division", "userunit_address": "Delhi Cantt",
                 "userunit_remarks": "Main armored unit", "uu_loc": 1, "password": "password123", "movedout": False},
                {"userunit_id": 2, "userunit_name": "2nd Infantry Brigade", "userunit_address": "Mumbai Garrison",
                 "userunit_remarks": "Infantry support unit", "uu_loc": 2, "password": "password456",
                 "movedout": False},
                {"userunit_id": 3, "userunit_name": "3rd Artillery Regiment", "userunit_address": "Pune Base",
                 "userunit_remarks": "Artillery operations", "uu_loc": 3, "password": "password789", "movedout": False},
                {"userunit_id": 4, "userunit_name": "4th Logistics Battalion", "userunit_address": "Chennai Station",
                 "userunit_remarks": "Logistics support", "uu_loc": 4, "password": "passwordabc", "movedout": True},
                {"userunit_id": 5, "userunit_name": "5th Transport Company", "userunit_address": "Kolkata Depot",
                 "userunit_remarks": "Transport operations", "uu_loc": 5, "password": "passwordxyz", "movedout": False}
            ]

            for record in tuserunit_data:
                conn.execute(tuserunit_query, record)
            conn.commit()
            print(f"✅ Inserted {len(tuserunit_data)} records into tUserUnit")

            # 2. Insert into tSubCat
            print("[2/6] Inserting into tSubCat...")
            tsubcat_query = text("""
                INSERT INTO tsubcat (subcatid, subcategoryname, categoryname, subcategorycode, mothersection, catasperFRS, remarks, oemname, oemaddress, oemcontactno, oememail)
                VALUES (:subcatid, :subcategoryname, :categoryname, :subcategorycode, :mothersection, :catasperFRS, :remarks, :oemname, :oemaddress, :oemcontactno, :oememail)
            """)

            tsubcat_data = [
                {"subcatid": 1, "subcategoryname": "Main Battle Tank", "categoryname": "B", "subcategorycode": "MBT",
                 "mothersection": 1, "catasperFRS": 1, "remarks": "Heavy armor", "oemname": "DefenseTech Ltd",
                 "oemaddress": "Delhi", "oemcontactno": "+91-11-12345678", "oememail": "info@defensetech.com"},
                {"subcatid": 2, "subcategoryname": "Infantry Fighting Vehicle", "categoryname": "B",
                 "subcategorycode": "IFV", "mothersection": 1, "catasperFRS": 2, "remarks": "Troop transport",
                 "oemname": "ArmorWorks Pvt", "oemaddress": "Mumbai", "oemcontactno": "+91-22-87654321",
                 "oememail": "sales@armorworks.com"},
                {"subcatid": 3, "subcategoryname": "Armored Personnel Carrier", "categoryname": "B",
                 "subcategorycode": "APC", "mothersection": 2, "catasperFRS": 3, "remarks": "Personnel transport",
                 "oemname": "MilitaryVeh Corp", "oemaddress": "Pune", "oemcontactno": "+91-20-11223344",
                 "oememail": "contact@militaryveh.com"},
                {"subcatid": 4, "subcategoryname": "Self Propelled Artillery", "categoryname": "B",
                 "subcategorycode": "SPA", "mothersection": 3, "catasperFRS": 4, "remarks": "Mobile artillery",
                 "oemname": "ArtilleryTech", "oemaddress": "Chennai", "oemcontactno": "+91-44-55667788",
                 "oememail": "support@artillerytech.com"},
                {"subcatid": 5, "subcategoryname": "Military Truck", "categoryname": "B", "subcategorycode": "TRK",
                 "mothersection": 4, "catasperFRS": 5, "remarks": "Logistics vehicle", "oemname": "TruckMakers Ltd",
                 "oemaddress": "Kolkata", "oemcontactno": "+91-33-99887766", "oememail": "info@truckmakers.com"}
            ]

            for record in tsubcat_data:
                conn.execute(tsubcat_query, record)
            conn.commit()
            print(f"✅ Inserted {len(tsubcat_data)} records into tSubCat")

            # 3. Insert into tFaults
            print("[3/6] Inserting into tFaults...")
            tfaults_query = text("""
                INSERT INTO tfaults (faultid, subcatcode, refsubsystem, faultcode, faultnumber, faults, repairtimeh, repairtimem, remarks, nooftrademen, workdone, critical)
                VALUES (:faultid, :subcatcode, :refsubsystem, :faultcode, :faultnumber, :faults, :repairtimeh, :repairtimem, :remarks, :nooftrademen, :workdone, :critical)
            """)

            tfaults_data = [
                {"faultid": 1, "subcatcode": 1, "refsubsystem": 1, "faultcode": 101, "faultnumber": "F001",
                 "faults": "Engine overheating", "repairtimeh": 4, "repairtimem": 30,
                 "remarks": "Critical engine fault", "nooftrademen": 2, "workdone": "Replace radiator", "critical": 1},
                {"faultid": 2, "subcatcode": 1, "refsubsystem": 2, "faultcode": 102, "faultnumber": "F002",
                 "faults": "Transmission failure", "repairtimeh": 6, "repairtimem": 0, "remarks": "Gearbox malfunction",
                 "nooftrademen": 3, "workdone": "Repair transmission", "critical": 1},
                {"faultid": 3, "subcatcode": 2, "refsubsystem": 1, "faultcode": 201, "faultnumber": "F003",
                 "faults": "Brake system failure", "repairtimeh": 2, "repairtimem": 15, "remarks": "Safety critical",
                 "nooftrademen": 2, "workdone": "Replace brake pads", "critical": 1},
                {"faultid": 4, "subcatcode": 3, "refsubsystem": 3, "faultcode": 301, "faultnumber": "F004",
                 "faults": "Suspension damage", "repairtimeh": 3, "repairtimem": 45, "remarks": "Mobility issue",
                 "nooftrademen": 2, "workdone": "Repair suspension", "critical": 0},
                {"faultid": 5, "subcatcode": 4, "refsubsystem": 2, "faultcode": 401, "faultnumber": "F005",
                 "faults": "Electrical short circuit", "repairtimeh": 1, "repairtimem": 30,
                 "remarks": "Power system fault", "nooftrademen": 1, "workdone": "Replace wiring", "critical": 0}
            ]

            for record in tfaults_data:
                conn.execute(tfaults_query, record)
            conn.commit()
            print(f"✅ Inserted {len(tfaults_data)} records into tFaults")

            # 4. Insert into tEqptRecord
            print("[4/6] Inserting into tEqptRecord...")
            teqptrecord_query = text("""
                INSERT INTO teqptrecord (id, erid, cat, eqptstatus, eqptfunctionalstatus, userunit, catasperFRS, issuetype, dataentryclkname, datedataentry, code, regnno, nomenclature, dtofissue, inkm)
                VALUES (:id, :erid, :cat, :eqptstatus, :eqptfunctionalstatus, :userunit, :catasperFRS, :issuetype, :dataentryclkname, :datedataentry, :code, :regnno, :nomenclature, :dtofissue, :inkm)
            """)

            teqptrecord_data = [
                {"id": 1, "erid": "ER001", "cat": 1, "eqptstatus": 1, "eqptfunctionalstatus": 1, "userunit": 1,
                 "catasperFRS": 1, "issuetype": 1, "dataentryclkname": 1, "datedataentry": datetime(2023, 1, 15),
                 "code": 1001, "regnno": "DEF12345", "nomenclature": "T-90 Main Battle Tank",
                 "dtofissue": datetime(2020, 3, 15), "inkm": "45000"},
                {"id": 2, "erid": "ER002", "cat": 2, "eqptstatus": 2, "eqptfunctionalstatus": 2, "userunit": 2,
                 "catasperFRS": 2, "issuetype": 2, "dataentryclkname": 2, "datedataentry": datetime(2023, 2, 20),
                 "code": 1002, "regnno": "INF67890", "nomenclature": "BMP-2 Infantry Fighting Vehicle",
                 "dtofissue": datetime(2019, 6, 10), "inkm": "38000"},
                {"id": 3, "erid": "ER003", "cat": 3, "eqptstatus": 1, "eqptfunctionalstatus": 1, "userunit": 3,
                 "catasperFRS": 3, "issuetype": 1, "dataentryclkname": 3, "datedataentry": datetime(2023, 3, 25),
                 "code": 1003, "regnno": "APC11223", "nomenclature": "BTR-80 Armored Personnel Carrier",
                 "dtofissue": datetime(2021, 8, 22), "inkm": "25000"},
                {"id": 4, "erid": "ER004", "cat": 4, "eqptstatus": 3, "eqptfunctionalstatus": 2, "userunit": 4,
                 "catasperFRS": 4, "issuetype": 3, "dataentryclkname": 4, "datedataentry": datetime(2023, 4, 10),
                 "code": 1004, "regnno": "ART44556", "nomenclature": "2S19 Self Propelled Artillery",
                 "dtofissue": datetime(2018, 12, 5), "inkm": "52000"},
                {"id": 5, "erid": "ER005", "cat": 5, "eqptstatus": 2, "eqptfunctionalstatus": 1, "userunit": 5,
                 "catasperFRS": 5, "issuetype": 2, "dataentryclkname": 5, "datedataentry": datetime(2023, 5, 18),
                 "code": 1005, "regnno": "TRK77889", "nomenclature": "TATRA Military Truck",
                 "dtofissue": datetime(2022, 1, 30), "inkm": "78000"}
            ]

            for record in teqptrecord_data:
                conn.execute(teqptrecord_query, record)
            conn.commit()
            print(f"✅ Inserted {len(teqptrecord_data)} records into tEqptRecord")

            # 5. Insert into JobCard
            print("[5/6] Inserting into JobCard...")
            jobcard_query = text("""
                INSERT INTO jobcard (id, jobcardno, jobcarddate, referid, wordorderno, wordorderdate, eqpttimein, dues, inkm)
                VALUES (:id, :jobcardno, :jobcarddate, :referid, :wordorderno, :wordorderdate, :eqpttimein, :dues, :inkm)
            """)

            jobcard_data = [
                {"id": 1, "jobcardno": "JC001/2023", "jobcarddate": datetime(2023, 6, 1), "referid": 1,
                 "wordorderno": "WO001", "wordorderdate": datetime(2023, 5, 30),
                 "eqpttimein": datetime(2023, 6, 1, 9, 0), "dues": "Maintenance", "inkm": 45000},
                {"id": 2, "jobcardno": "JC002/2023", "jobcarddate": datetime(2023, 6, 15), "referid": 2,
                 "wordorderno": "WO002", "wordorderdate": datetime(2023, 6, 14),
                 "eqpttimein": datetime(2023, 6, 15, 10, 30), "dues": "Repair", "inkm": 38500},
                {"id": 3, "jobcardno": "JC003/2023", "jobcarddate": datetime(2023, 7, 3), "referid": 3,
                 "wordorderno": "WO003", "wordorderdate": datetime(2023, 7, 2),
                 "eqpttimein": datetime(2023, 7, 3, 8, 45), "dues": "Inspection", "inkm": 25200},
                {"id": 4, "jobcardno": "JC004/2023", "jobcarddate": datetime(2023, 7, 20), "referid": 4,
                 "wordorderno": "WO004", "wordorderdate": datetime(2023, 7, 19),
                 "eqpttimein": datetime(2023, 7, 20, 11, 15), "dues": "Overhaul", "inkm": 52800},
                {"id": 5, "jobcardno": "JC005/2023", "jobcarddate": datetime(2023, 8, 5), "referid": 5,
                 "wordorderno": "WO005", "wordorderdate": datetime(2023, 8, 4),
                 "eqpttimein": datetime(2023, 8, 5, 14, 20), "dues": "Service", "inkm": 78500}
            ]

            for record in jobcard_data:
                conn.execute(jobcard_query, record)
            conn.commit()
            print(f"✅ Inserted {len(jobcard_data)} records into JobCard")

            # 6. Insert into JobCardDetails
            print("[6/6] Inserting into JobCardDetails...")
            jobcarddetails_query = text("""
                INSERT INTO jobcarddetails (id, refjobno, tcode, fault, workdone, repairdate, timetakenh, timetakenm, nooftrademen, entryclk, critical)
                VALUES (:id, :refjobno, :tcode, :fault, :workdone, :repairdate, :timetakenh, :timetakenm, :nooftrademen, :entryclk, :critical)
            """)

            jobcarddetails_data = [
                {"id": 1, "refjobno": 1, "tcode": 101, "fault": 1, "workdone": "Replaced radiator and coolant system",
                 "repairdate": datetime(2023, 6, 2), "timetakenh": 4, "timetakenm": 30, "nooftrademen": 2,
                 "entryclk": 1, "critical": 1},
                {"id": 2, "refjobno": 1, "tcode": 102, "fault": 2, "workdone": "Serviced transmission",
                 "repairdate": datetime(2023, 6, 2), "timetakenh": 2, "timetakenm": 0, "nooftrademen": 1, "entryclk": 1,
                 "critical": 0},
                {"id": 3, "refjobno": 2, "tcode": 201, "fault": 3, "workdone": "Replaced brake pads and fluid",
                 "repairdate": datetime(2023, 6, 16), "timetakenh": 2, "timetakenm": 15, "nooftrademen": 2,
                 "entryclk": 2, "critical": 1},
                {"id": 4, "refjobno": 3, "tcode": 301, "fault": 4, "workdone": "Repaired front suspension",
                 "repairdate": datetime(2023, 7, 4), "timetakenh": 3, "timetakenm": 45, "nooftrademen": 2,
                 "entryclk": 3, "critical": 0},
                {"id": 5, "refjobno": 4, "tcode": 401, "fault": 5, "workdone": "Fixed electrical wiring",
                 "repairdate": datetime(2023, 7, 21), "timetakenh": 1, "timetakenm": 30, "nooftrademen": 1,
                 "entryclk": 4, "critical": 0},
                {"id": 6, "refjobno": 5, "tcode": 101, "fault": 1, "workdone": "Engine maintenance",
                 "repairdate": datetime(2023, 8, 6), "timetakenh": 3, "timetakenm": 0, "nooftrademen": 2, "entryclk": 5,
                 "critical": 0}
            ]

            for record in jobcarddetails_data:
                conn.execute(jobcarddetails_query, record)
            conn.commit()
            print(f"✅ Inserted {len(jobcarddetails_data)} records into JobCardDetails")

        print("\n🎉 All data inserted successfully!")

        # Verify data insertion
        print("\n📊 VERIFICATION:")
        tables = ['tuserunit', 'tsubcat', 'teqptrecord', 'jobcard', 'jobcarddetails', 'tfaults']
        for table in tables:
            df = pd.read_sql(f'SELECT COUNT(*) as count FROM "{table}"', engine)
            print(f"✅ {table}: {df['count'][0]} records")

    except Exception as e:
        print(f"❌ Data insertion failed: {e}")
        return False


if __name__ == "__main__":
    insert_sample_data()
