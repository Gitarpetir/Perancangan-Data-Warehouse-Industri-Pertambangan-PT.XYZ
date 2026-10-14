"""
Automated smoke/data-quality tests for PT XYZ Data Warehouse.
Run from the project root:
    python -m unittest -v test_dw.py

Read-only: tests do not insert, update, or delete database rows.
Connection settings are loaded through standalone_etl.get_sql_connection().
"""

import unittest
from standalone_etl import get_sql_connection


EXPECTED_FACTS = {
    "fact.FactEquipmentUsage": ("equipment_usage_id", "staging.EquipmentUsage", "equipment_usage_id"),
    "fact.FactProduction": ("production_id", "staging.Production", "production_id"),
    "fact.FactFinancialTransaction": ("transaction_id", "staging.FinancialTransaction", "id"),
}

REQUIRED_COLUMNS = {
    "dim.DimTime": {
        "time_key", "time_id", "date", "day_of_month", "day_name",
        "month", "month_name", "quarter", "year", "is_weekend",
    },
    "dim.DimSite": {
        "site_key", "site_id", "site_name", "region", "latitude", "longitude",
    },
    "dim.DimEquipment": {
        "equipment_key", "equipment_name", "equipment_type", "manufacture",
        "model", "capacity", "purchase_date",
    },
    "dim.DimMaterial": {
        "material_key", "material_id", "material_name", "material_type",
        "unit_of_measure",
    },
    "dim.DimEmployee": {
        "employee_key", "employee_id", "employee_name", "position",
        "department", "status", "hire_date",
    },
    "dim.DimShift": {"shift_key", "shift_id", "shift_name", "start_time", "end_time"},
    "dim.DimProject": {
        "project_key", "project_id", "project_name", "project_manager",
        "status", "start_date", "end_date",
    },
    "dim.DimAccount": {
        "account_key", "account_id", "account_name", "account_type", "budget_category",
    },
    "fact.FactEquipmentUsage": {
        "usage_key", "equipment_usage_id", "time_key", "site_key", "equipment_key",
        "operating_hours", "downtime_hours", "fuel_consumption", "maintenance_cost",
    },
    "fact.FactProduction": {
        "production_key", "production_id", "time_key", "site_key", "material_key",
        "employee_key", "shift_key", "produced_volume", "unit_cost", "total_cost",
        "material_quantity",
    },
    "fact.FactFinancialTransaction": {
        "transaction_key", "transaction_id", "time_key", "site_key", "project_key",
        "account_key", "budgeted_cost", "actual_cost", "variance_amount",
        "variance_percentage", "variance_status", "account_cost",
    },
    "staging.EquipmentUsage": {"equipment_usage_id", "date", "equipment_name"},
    "staging.Production": {"production_id", "date", "site_id", "material_id"},
    "staging.FinancialTransaction": {"id", "date", "site_id", "project_id", "account_id"},
}

DATE_CHECKS = [
    (
        "EquipmentUsage",
        """
        SELECT COUNT(*)
        FROM staging.EquipmentUsage s
        JOIN fact.FactEquipmentUsage f
          ON f.equipment_usage_id = s.equipment_usage_id
        JOIN dim.DimTime t ON t.time_key = f.time_key
        WHERE CAST(s.[date] AS date) <> CAST(t.[date] AS date)
        """,
    ),
    (
        "Production",
        """
        SELECT COUNT(*)
        FROM staging.Production s
        JOIN fact.FactProduction f ON f.production_id = s.production_id
        JOIN dim.DimTime t ON t.time_key = f.time_key
        WHERE CAST(s.[date] AS date) <> CAST(t.[date] AS date)
        """,
    ),
    (
        "FinancialTransaction",
        """
        SELECT COUNT(*)
        FROM staging.FinancialTransaction s
        JOIN fact.FactFinancialTransaction f ON f.transaction_id = s.id
        JOIN dim.DimTime t ON t.time_key = f.time_key
        WHERE CAST(s.[date] AS date) <> CAST(t.[date] AS date)
        """,
    ),
]

MEASURE_CHECKS = [
    ("fact.FactEquipmentUsage", "operating_hours", "staging.EquipmentUsage", "operating_hours", "equipment_usage_id", "equipment_usage_id"),
    ("fact.FactEquipmentUsage", "downtime_hours", "staging.EquipmentUsage", "downtime_hours", "equipment_usage_id", "equipment_usage_id"),
    ("fact.FactEquipmentUsage", "fuel_consumption", "staging.EquipmentUsage", "fuel_consumption", "equipment_usage_id", "equipment_usage_id"),
    ("fact.FactEquipmentUsage", "maintenance_cost", "staging.EquipmentUsage", "maintenance_cost", "equipment_usage_id", "equipment_usage_id"),
    ("fact.FactProduction", "produced_volume", "staging.Production", "produced_volume", "production_id", "production_id"),
    ("fact.FactProduction", "unit_cost", "staging.Production", "unit_cost", "production_id", "production_id"),
    ("fact.FactProduction", "material_quantity", "staging.Production", "quantity", "production_id", "production_id"),
    ("fact.FactFinancialTransaction", "budgeted_cost", "staging.FinancialTransaction", "budgeted_cost", "transaction_id", "id"),
    ("fact.FactFinancialTransaction", "actual_cost", "staging.FinancialTransaction", "actual_cost", "transaction_id", "id"),
    ("fact.FactFinancialTransaction", "account_cost", "staging.FinancialTransaction", "cost", "transaction_id", "id"),
]


class DataWarehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = get_sql_connection()
        cls.cur = cls.conn.cursor()
        cls.cur.execute("SELECT DB_NAME()")
        cls.database_name = cls.cur.fetchone()[0]

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()

    def test_connected_to_expected_database(self):
        self.assertEqual(self.database_name, "PTXYZ_DataWarehouse")

    def test_required_tables_and_columns_exist(self):
        for full_table, expected_columns in REQUIRED_COLUMNS.items():
            schema, table = full_table.split(".")
            self.cur.execute(
                """
                SELECT c.name
                FROM sys.columns c
                JOIN sys.tables t ON t.object_id = c.object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = %s AND t.name = %s
                """,
                (schema, table),
            )
            actual_columns = {row[0].lower() for row in self.cur.fetchall()}
            self.assertTrue(actual_columns, f"Tabel tidak ditemukan: {full_table}")
            missing = expected_columns - actual_columns
            self.assertFalse(
                missing,
                f"{full_table} kehilangan kolom yang diwajibkan: {sorted(missing)}",
            )

    def test_staging_and_fact_counts_match_and_ids_are_unique(self):
        for fact_table, (fact_id, staging_table, staging_id) in EXPECTED_FACTS.items():
            self.cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT [{staging_id}]) FROM {staging_table}")
            staging_count, staging_unique = self.cur.fetchone()
            self.cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT [{fact_id}]) FROM {fact_table}")
            fact_count, fact_unique = self.cur.fetchone()

            self.assertEqual(staging_count, 1150, f"{staging_table} seharusnya berisi 1150 baris")
            self.assertEqual(staging_count, staging_unique, f"ID sumber tidak unik di {staging_table}")
            self.assertEqual(fact_count, staging_count, f"Jumlah fact berbeda dari staging: {fact_table}")
            self.assertEqual(fact_count, fact_unique, f"ID duplikat di {fact_table}")

    def test_fact_foreign_keys_have_no_orphans(self):
        # Check all fact-to-dimension relationships used by the current schema.
        checks = [
            ("FactEquipmentUsage -> DimTime", "fact.FactEquipmentUsage f LEFT JOIN dim.DimTime d ON d.time_key=f.time_key", "d.time_key"),
            ("FactEquipmentUsage -> DimSite", "fact.FactEquipmentUsage f LEFT JOIN dim.DimSite d ON d.site_key=f.site_key", "d.site_key"),
            ("FactEquipmentUsage -> DimEquipment", "fact.FactEquipmentUsage f LEFT JOIN dim.DimEquipment d ON d.equipment_key=f.equipment_key", "d.equipment_key"),
            ("FactProduction -> DimTime", "fact.FactProduction f LEFT JOIN dim.DimTime d ON d.time_key=f.time_key", "d.time_key"),
            ("FactProduction -> DimSite", "fact.FactProduction f LEFT JOIN dim.DimSite d ON d.site_key=f.site_key", "d.site_key"),
            ("FactProduction -> DimMaterial", "fact.FactProduction f LEFT JOIN dim.DimMaterial d ON d.material_key=f.material_key", "d.material_key"),
            ("FactProduction -> DimEmployee", "fact.FactProduction f LEFT JOIN dim.DimEmployee d ON d.employee_key=f.employee_key", "d.employee_key"),
            ("FactProduction -> DimShift", "fact.FactProduction f LEFT JOIN dim.DimShift d ON d.shift_key=f.shift_key", "d.shift_key"),
            ("FactFinancialTransaction -> DimTime", "fact.FactFinancialTransaction f LEFT JOIN dim.DimTime d ON d.time_key=f.time_key", "d.time_key"),
            ("FactFinancialTransaction -> DimSite", "fact.FactFinancialTransaction f LEFT JOIN dim.DimSite d ON d.site_key=f.site_key", "d.site_key"),
            ("FactFinancialTransaction -> DimProject", "fact.FactFinancialTransaction f LEFT JOIN dim.DimProject d ON d.project_key=f.project_key", "d.project_key"),
            ("FactFinancialTransaction -> DimAccount", "fact.FactFinancialTransaction f LEFT JOIN dim.DimAccount d ON d.account_key=f.account_key", "d.account_key"),
        ]
        for label, join_sql, parent_key in checks:
            self.cur.execute(f"SELECT COUNT(*) FROM {join_sql} WHERE {parent_key} IS NULL")
            orphan_count = self.cur.fetchone()[0]
            self.assertEqual(orphan_count, 0, f"{label} memiliki {orphan_count} orphan")

    def test_fact_dates_match_staging_dates(self):
        for label, query in DATE_CHECKS:
            self.cur.execute(query)
            mismatches = self.cur.fetchone()[0]
            self.assertEqual(mismatches, 0, f"{label}: ada tanggal fact yang tidak cocok")

    def test_measure_sums_match_staging(self):
        for fact_table, fact_measure, source_table, source_measure, fact_id, source_id in MEASURE_CHECKS:
            self.cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(CAST(f.[{fact_measure}] AS decimal(38, 6))), 0),
                    COALESCE(SUM(CAST(s.[{source_measure}] AS decimal(38, 6))), 0)
                FROM {source_table} s
                JOIN {fact_table} f ON f.[{fact_id}] = s.[{source_id}]
                """
            )
            fact_sum, source_sum = self.cur.fetchone()
            self.assertEqual(
                fact_sum,
                source_sum,
                f"SUM {fact_table}.{fact_measure} tidak sama dengan staging",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
