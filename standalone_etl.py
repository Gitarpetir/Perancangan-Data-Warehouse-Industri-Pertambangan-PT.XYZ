#!/usr/bin/env python3
"""
Standalone ETL Pipeline for PT XYZ Data Warehouse
This script runs the complete ETL pipeline without Airflow dependencies
"""

import pandas as pd
import pymssql
import logging
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_execution.log'),
        logging.StreamHandler()
    ]
)


def get_sql_connection():
    """Connect to the PT XYZ Data Warehouse."""

    sql_password = os.getenv("MSSQL_SA_PASSWORD")

    if not sql_password:
        raise RuntimeError(
            "MSSQL_SA_PASSWORD tidak ditemukan. "
            "Periksa file .env."
        )

    return pymssql.connect(
        server="localhost",
        port=1434,
        database="PTXYZ_DataWarehouse",
        user="sa",
        password=sql_password,
        timeout=30,
        login_timeout=10
    )

def extract_and_load_to_staging():
    """Extract data from CSV files and load to staging tables"""
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Clear staging tables
        logging.info("Clearing staging tables...")
        cursor.execute("TRUNCATE TABLE staging.EquipmentUsage")
        cursor.execute("TRUNCATE TABLE staging.Production") 
        cursor.execute("TRUNCATE TABLE staging.FinancialTransaction")
        
        # Load Equipment Usage data
        logging.info("Loading Equipment Usage data...")
        equipment_df = pd.read_csv('data/dataset_alat_berat_dw.csv')
        equipment_df['purchase_date'] = pd.to_datetime(equipment_df['purchase_date']).dt.date
        equipment_df['date'] = pd.to_datetime(equipment_df['date']).dt.date
        equipment_df['created_at'] = pd.to_datetime(equipment_df['created_at'])
        
        count = 0
        for _, row in equipment_df.iterrows():
            cursor.execute("""
                INSERT INTO staging.EquipmentUsage (
                    equipment_usage_id, time_id, date, day, day_name, month, year,
                    site_name, region, latitude, longitude, equipment_name, 
                    equipment_type, manufacture, model, capacity, purchase_date,
                    operating_hours, downtime_hours, fuel_consumption, 
                    maintenance_cost, created_at, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
            count += 1
            if count % 100 == 0:
                logging.info(f"Loaded {count} equipment records...")
        
        logging.info(f"Loaded {count} equipment usage records")
        
        # Load Production data
        logging.info("Loading Production data...")
        production_df = pd.read_csv('data/dataset_production.csv')
        production_df['date'] = pd.to_datetime(production_df['date']).dt.date
        production_df['hire_date'] = pd.to_datetime(production_df['hire_date']).dt.date
        
        count = 0
        for _, row in production_df.iterrows():
            cursor.execute("""
                INSERT INTO staging.Production (
                    production_id, time_id, site_id, material_id, employee_id, shift_id,
                    produced_volume, unit_cost, date, day, month, year, day_name,
                    site_name, region, latitude, longitude, material_name, material_type,
                    unit_of_measure, quantity, employee_name, position, department,
                    status, hire_date, shift_name, start_time, end_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
            count += 1
            if count % 100 == 0:
                logging.info(f"Loaded {count} production records...")
        
        logging.info(f"Loaded {count} production records")
        
        # Load Financial Transaction data
        logging.info("Loading Financial Transaction data...")
        transaction_df = pd.read_csv('data/dataset_transaksi.csv')
        transaction_df['created_at'] = pd.to_datetime(transaction_df['created_at'])
        transaction_df['date'] = pd.to_datetime(transaction_df['date'], format='%Y%m%d').dt.date
        transaction_df['start_date'] = pd.to_datetime(transaction_df['start_date']).dt.date
        transaction_df['end_date'] = pd.to_datetime(transaction_df['end_date']).dt.date
        
        count = 0
        for _, row in transaction_df.iterrows():
            cursor.execute("""
                INSERT INTO staging.FinancialTransaction (
                    id, time_id, site_id, project_id, account_id, variance,
                    budgeted_cost, actual_cost, created_at, created_by, date,
                    day, day_name, month, year, site_name, region, latitude,
                    longitude, project_name, project_manager, status, start_date,
                    end_date, account_name, account_type, budget_category, cost
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
            count += 1
            if count % 100 == 0:
                logging.info(f"Loaded {count} transaction records...")
        
        logging.info(f"Loaded {count} transaction records")
        
        conn.commit()
        conn.close()
        
        logging.info("Data successfully loaded to staging tables")
        return "Staging load completed"
        
    except Exception as e:
        logging.error(f"Error in staging load: {str(e)}")
        raise

def transform_and_load_dimensions():
    """Transform and load dimension tables"""
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        logging.info("Loading Time Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimTime (time_id, date, day_of_month, day_name, month, month_name, quarter, year, is_weekend)
            SELECT DISTINCT 
                time_id, 
                date,
                day,
                day_name,
                month,
                DATENAME(MONTH, date) as month_name,
                DATEPART(QUARTER, date) as quarter,
                year,
                CASE WHEN DATENAME(WEEKDAY, date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END as is_weekend
            FROM staging.EquipmentUsage e
            WHERE NOT EXISTS (SELECT 1 FROM dim.DimTime d WHERE d.time_id = e.time_id)
            UNION
            SELECT DISTINCT 
                time_id, 
                date,
                day,
                day_name,
                month,
                DATENAME(MONTH, date) as month_name,
                DATEPART(QUARTER, date) as quarter,
                year,
                CASE WHEN DATENAME(WEEKDAY, date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END as is_weekend
            FROM staging.Production p
            WHERE NOT EXISTS (SELECT 1 FROM dim.DimTime d WHERE d.time_id = p.time_id)
            UNION
            SELECT DISTINCT 
                time_id, 
                date,
                day,
                day_name,
                month,
                DATENAME(MONTH, date) as month_name,
                DATEPART(QUARTER, date) as quarter,
                year,
                CASE WHEN DATENAME(WEEKDAY, date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END as is_weekend
            FROM staging.FinancialTransaction f
            WHERE NOT EXISTS (SELECT 1 FROM dim.DimTime d WHERE d.time_id = f.time_id)
        """)
        
        logging.info("Loading Site Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimSite (site_id, site_name, region, latitude, longitude)
            SELECT DISTINCT 
                COALESCE(p.site_id, ROW_NUMBER() OVER (ORDER BY e.site_name)) as site_id,
                COALESCE(e.site_name, p.site_name, f.site_name) as site_name,
                COALESCE(e.region, p.region, f.region) as region,
                COALESCE(e.latitude, p.latitude, f.latitude) as latitude,
                COALESCE(e.longitude, p.longitude, f.longitude) as longitude
            FROM staging.EquipmentUsage e
            FULL OUTER JOIN staging.Production p ON e.site_name = p.site_name
            FULL OUTER JOIN staging.FinancialTransaction f ON COALESCE(e.site_name, p.site_name) = f.site_name
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimSite d 
                WHERE d.site_name = COALESCE(e.site_name, p.site_name, f.site_name)
            )
        """)
        
        logging.info("Loading Equipment Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimEquipment (equipment_name, equipment_type, manufacture, model, capacity, purchase_date)
            SELECT DISTINCT 
                equipment_name, equipment_type, manufacture, model, capacity, purchase_date
            FROM staging.EquipmentUsage e
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimEquipment d 
                WHERE d.equipment_name = e.equipment_name
            )
        """)
        
        logging.info("Loading Material Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimMaterial (material_id, material_name, material_type, unit_of_measure)
            SELECT DISTINCT 
                material_id, material_name, material_type, unit_of_measure
            FROM staging.Production p
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimMaterial d 
                WHERE d.material_id = p.material_id
            )
        """)
        
        logging.info("Loading Employee Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimEmployee (employee_id, employee_name, position, department, status, hire_date)
            SELECT DISTINCT 
                employee_id, employee_name, position, department, status, hire_date
            FROM staging.Production p
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimEmployee d 
                WHERE d.employee_id = p.employee_id
            )
        """)
        
        logging.info("Loading Shift Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimShift (shift_id, shift_name, start_time, end_time)
            SELECT DISTINCT 
                shift_id, shift_name, start_time, end_time
            FROM staging.Production p
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimShift d 
                WHERE d.shift_id = p.shift_id
            )
        """)
        
        logging.info("Loading Project Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimProject (project_id, project_name, project_manager, status, start_date, end_date)
            SELECT DISTINCT 
                project_id, project_name, project_manager, status, start_date, end_date
            FROM staging.FinancialTransaction f
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimProject d 
                WHERE d.project_id = f.project_id
            )
        """)
        
        logging.info("Loading Account Dimension...")
        cursor.execute("""
            INSERT INTO dim.DimAccount (account_id, account_name, account_type, budget_category)
            SELECT DISTINCT 
                account_id, account_name, account_type, budget_category
            FROM staging.FinancialTransaction f
            WHERE NOT EXISTS (
                SELECT 1 FROM dim.DimAccount d 
                WHERE d.account_id = f.account_id
            )
        """)
        
        conn.commit()
        conn.close()
        
        logging.info("Dimension tables loaded successfully")
        return "Dimension load completed"
        
    except Exception as e:
        logging.error(f"Error loading dimensions: {str(e)}")
        raise

def load_fact_tables():
    """Full-refresh fact loading with transaction, locking, and validation.

    The three fact tables are rebuilt from staging in one transaction.
    Dimension and staging tables are not modified by this function.
    """
    conn = None
    cursor = None
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        conn.autocommit(False)

        logging.info("Starting validated fact-table refresh...")

        # Ensure two executions of this corrected loader do not run together.
        cursor.execute("""
            DECLARE @result INT;
            EXEC @result = sp_getapplock
                @Resource = 'PTXYZ_Fact_Load',
                @LockMode = 'Exclusive',
                @LockOwner = 'Transaction',
                @LockTimeout = 30000;
            SELECT @result;
        """)
        lock_result = cursor.fetchone()[0]
        if lock_result < 0:
            raise RuntimeError(
                f"Tidak bisa memperoleh lock ETL (kode {lock_result}). "
                "Kemungkinan proses ETL lain sedang berjalan."
            )

        # Acquire exclusive table locks, held until commit/rollback. This also
        # prevents concurrent writers from modifying these fact tables mid-load.
        fact_tables = [
            "fact.FactEquipmentUsage",
            "fact.FactProduction",
            "fact.FactFinancialTransaction",
        ]
        for table in fact_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WITH (TABLOCKX, HOLDLOCK)")
            cursor.fetchone()

        # This is a controlled full refresh of fact rows only.
        for table in fact_tables:
            cursor.execute(f"DELETE FROM {table}")

        expected_counts = {}
        for table, source in [
            ("fact.FactEquipmentUsage", "staging.EquipmentUsage"),
            ("fact.FactProduction", "staging.Production"),
            ("fact.FactFinancialTransaction", "staging.FinancialTransaction"),
        ]:
            cursor.execute(f"SELECT COUNT(*) FROM {source}")
            expected_counts[table] = cursor.fetchone()[0]

        logging.info("Loading FactEquipmentUsage...")
        cursor.execute("""
            INSERT INTO fact.FactEquipmentUsage (
                equipment_usage_id, time_key, site_key, equipment_key,
                operating_hours, downtime_hours, fuel_consumption, maintenance_cost
            )
            SELECT DISTINCT
                e.equipment_usage_id,
                t.time_key,
                s.site_key,
                eq.equipment_key,
                e.operating_hours,
                e.downtime_hours,
                e.fuel_consumption,
                e.maintenance_cost
            FROM staging.EquipmentUsage e
            JOIN dim.DimTime t ON e.[date] = t.[date]
            JOIN dim.DimSite s
              ON e.site_name = s.site_name
             AND e.region = s.region
             AND (e.latitude = s.latitude OR (e.latitude IS NULL AND s.latitude IS NULL))
             AND (e.longitude = s.longitude OR (e.longitude IS NULL AND s.longitude IS NULL))
             AND s.site_id < 0
            JOIN dim.DimEquipment eq ON e.equipment_name = eq.equipment_name
        """)
        inserted = cursor.rowcount
        logging.info("FactEquipmentUsage inserted rows: %s", inserted)
        if inserted != expected_counts["fact.FactEquipmentUsage"]:
            raise RuntimeError(
                "FactEquipmentUsage: jumlah hasil INSERT "
                f"{inserted}, tetapi staging berisi {expected_counts['fact.FactEquipmentUsage']}. "
                "Transaksi dibatalkan."
            )

        logging.info("Loading FactProduction...")
        cursor.execute("""
            INSERT INTO fact.FactProduction (
                production_id, time_key, site_key, material_key, employee_key, shift_key,
                produced_volume, unit_cost, material_quantity
            )
            SELECT DISTINCT
                p.production_id,
                t.time_key,
                s.site_key,
                m.material_key,
                e.employee_key,
                sh.shift_key,
                p.produced_volume,
                p.unit_cost,
                p.quantity
            FROM staging.Production p
            JOIN dim.DimTime t ON p.[date] = t.[date]
            JOIN dim.DimSite s
              ON p.site_id = s.site_id
             AND p.site_name = s.site_name
             AND p.region = s.region
             AND (p.latitude = s.latitude OR (p.latitude IS NULL AND s.latitude IS NULL))
             AND (p.longitude = s.longitude OR (p.longitude IS NULL AND s.longitude IS NULL))
            JOIN dim.DimMaterial m ON p.material_id = m.material_id
            JOIN dim.DimEmployee e ON p.employee_id = e.employee_id
            JOIN dim.DimShift sh ON p.shift_id = sh.shift_id
        """)
        inserted = cursor.rowcount
        logging.info("FactProduction inserted rows: %s", inserted)
        if inserted != expected_counts["fact.FactProduction"]:
            raise RuntimeError(
                "FactProduction: jumlah hasil INSERT "
                f"{inserted}, tetapi staging berisi {expected_counts['fact.FactProduction']}. "
                "Transaksi dibatalkan."
            )

        logging.info("Loading FactFinancialTransaction...")
        cursor.execute("""
            INSERT INTO fact.FactFinancialTransaction (
                transaction_id, time_key, site_key, project_key, account_key,
                budgeted_cost, actual_cost, account_cost
            )
            SELECT DISTINCT
                f.id AS transaction_id,
                t.time_key,
                s.site_key,
                p.project_key,
                a.account_key,
                f.budgeted_cost,
                f.actual_cost,
                f.cost
            FROM staging.FinancialTransaction f
            JOIN dim.DimTime t ON f.[date] = t.[date]
            JOIN dim.DimSite s
              ON f.site_id = s.site_id
             AND f.site_name = s.site_name
             AND f.region = s.region
             AND (f.latitude = s.latitude OR (f.latitude IS NULL AND s.latitude IS NULL))
             AND (f.longitude = s.longitude OR (f.longitude IS NULL AND s.longitude IS NULL))
            JOIN dim.DimProject p ON f.project_id = p.project_id
            JOIN dim.DimAccount a ON f.account_id = a.account_id
        """)
        inserted = cursor.rowcount
        logging.info("FactFinancialTransaction inserted rows: %s", inserted)
        if inserted != expected_counts["fact.FactFinancialTransaction"]:
            raise RuntimeError(
                "FactFinancialTransaction: jumlah hasil INSERT "
                f"{inserted}, tetapi staging berisi {expected_counts['fact.FactFinancialTransaction']}. "
                "Transaksi dibatalkan."
            )

        # Validate final counts and unique source IDs before committing.
        id_columns = {
            "fact.FactEquipmentUsage": "equipment_usage_id",
            "fact.FactProduction": "production_id",
            "fact.FactFinancialTransaction": "transaction_id",
        }
        for table in fact_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            id_column = id_columns[table]
            cursor.execute(f"SELECT COUNT(DISTINCT {id_column}) FROM {table}")
            unique_ids = cursor.fetchone()[0]
            expected = expected_counts[table]
            logging.info("%s: total=%s, unique IDs=%s, expected=%s", table, total, unique_ids, expected)
            if total != expected or unique_ids != expected:
                raise RuntimeError(
                    f"Validasi gagal pada {table}: total={total}, "
                    f"ID unik={unique_ids}, target={expected}. Transaksi dibatalkan."
                )

        conn.commit()
        logging.info("All fact tables refreshed and validated successfully.")
        return "Fact load completed; all counts validated"

    except Exception as e:
        if conn is not None:
            conn.rollback()
        logging.error("Error loading fact tables; transaction rolled back: %s", str(e))
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def data_quality_check():
    """Perform data quality checks"""
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Count records in each table
        tables_to_check = [
            ('staging.EquipmentUsage', 'Staging Equipment Usage'),
            ('staging.Production', 'Staging Production'),
            ('staging.FinancialTransaction', 'Staging Financial Transaction'),
            ('dim.DimTime', 'Time Dimension'),
            ('dim.DimSite', 'Site Dimension'),
            ('dim.DimEquipment', 'Equipment Dimension'),
            ('dim.DimMaterial', 'Material Dimension'),
            ('dim.DimEmployee', 'Employee Dimension'),
            ('dim.DimShift', 'Shift Dimension'),
            ('dim.DimProject', 'Project Dimension'),
            ('dim.DimAccount', 'Account Dimension'),
            ('fact.FactEquipmentUsage', 'Equipment Usage Fact'),
            ('fact.FactProduction', 'Production Fact'),
            ('fact.FactFinancialTransaction', 'Financial Transaction Fact')
        ]
        
        logging.info("Data Quality Check Results:")
        logging.info("=" * 50)
        
        for table_name, description in tables_to_check:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logging.info(f"{description:30} : {count:8,} records")
        
        conn.close()
        logging.info("=" * 50)
        return "Data quality check completed"
        
    except Exception as e:
        logging.error(f"Error in data quality check: {str(e)}")
        raise

def run_complete_etl():
    """Execute the complete ETL pipeline"""
    try:
        logging.info("ðŸš€ Starting PT XYZ Complete ETL Pipeline...")
        
        # Step 1: Extract and Load to Staging
        logging.info("ðŸ“¥ Step 1: Extracting and loading data to staging tables...")
        result1 = extract_and_load_to_staging()
        logging.info(f"âœ… Step 1 completed: {result1}")
        
        time.sleep(2)  # Brief pause between steps
        
        # Step 2: Transform and Load Dimensions
        logging.info("ðŸ”„ Step 2: Transforming and loading dimension tables...")
        result2 = transform_and_load_dimensions()
        logging.info(f"âœ… Step 2 completed: {result2}")
        
        time.sleep(2)
        
        # Step 3: Load Fact Tables
        logging.info("ðŸ“Š Step 3: Loading fact tables...")
        result3 = load_fact_tables()
        logging.info(f"âœ… Step 3 completed: {result3}")
        
        time.sleep(2)
        
        # Step 4: Data Quality Check
        logging.info("ðŸ” Step 4: Running data quality checks...")
        result4 = data_quality_check()
        logging.info(f"âœ… Step 4 completed: {result4}")
        
        logging.info("ðŸŽ‰ Complete ETL Pipeline Successfully Executed!")
        logging.info("ðŸ“ˆ Data Warehouse is now ready for analytics and visualization!")
        
        return True
        
    except Exception as e:
        logging.error(f"âŒ ETL Pipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    logging.info("=" * 80)
    logging.info("PT XYZ Data Warehouse - Complete ETL Pipeline Execution")
    logging.info("=" * 80)
    
    success = run_complete_etl()
    
    if success:
        logging.info("âœ… ETL Pipeline completed successfully!")
        logging.info("ðŸŒ You can now access:")
        logging.info("   - Airflow UI: http://localhost:8080")
        logging.info("   - Grafana: http://localhost:3000")
        logging.info("   - Superset: http://localhost:8088")
        logging.info("   - Metabase: http://localhost:3001")
        logging.info("   - Jupyter: http://localhost:8888")
    else:
        logging.error("âŒ ETL Pipeline failed!")

