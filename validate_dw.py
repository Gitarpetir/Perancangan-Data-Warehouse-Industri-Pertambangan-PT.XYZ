"""
Validasi kualitas data warehouse PT XYZ.
Skrip ini hanya membaca data; tidak menghapus atau mengubah tabel.
Jalankan dari folder project yang berisi .env dan standalone_etl.py.
"""

from decimal import Decimal
from standalone_etl import get_sql_connection


FACTS = [
    {
        "table": "fact.FactEquipmentUsage",
        "source_table": "staging.EquipmentUsage",
        "fact_id": "equipment_usage_id",
        "source_id": "equipment_usage_id",
        "measures": [
            ("operating_hours", "operating_hours"),
            ("downtime_hours", "downtime_hours"),
            ("fuel_consumption", "fuel_consumption"),
            ("maintenance_cost", "maintenance_cost"),
        ],
    },
    {
        "table": "fact.FactProduction",
        "source_table": "staging.Production",
        "fact_id": "production_id",
        "source_id": "production_id",
        "measures": [
            ("produced_volume", "produced_volume"),
            ("unit_cost", "unit_cost"),
            ("material_quantity", "quantity"),
        ],
    },
    {
        "table": "fact.FactFinancialTransaction",
        "source_table": "staging.FinancialTransaction",
        "fact_id": "transaction_id",
        "source_id": "id",
        "measures": [
            ("budgeted_cost", "budgeted_cost"),
            ("actual_cost", "actual_cost"),
            ("account_cost", "cost"),
        ],
    },
]


def fmt(value):
    if value is None:
        return "NULL"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def main():
    conn = get_sql_connection()
    cur = conn.cursor()

    errors = []
    print("=" * 72)
    print("VALIDASI DATA WAREHOUSE PT XYZ")
    print("Mode: READ-ONLY (tidak mengubah data)")
    cur.execute("SELECT @@SERVERNAME, DB_NAME()")
    print("Server/database:", cur.fetchone())
    print("=" * 72)

    # 1. Row counts and uniqueness of source IDs
    print("\n[1] JUMLAH BARIS DAN KEUNIKAN ID")
    for item in FACTS:
        table = item["table"]
        source_table = item["source_table"]
        fact_id = item["fact_id"]
        source_id = item["source_id"]

        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT [{source_id}]) FROM {source_table}")
        source_count, source_unique = cur.fetchone()

        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT [{fact_id}]) FROM {table}")
        fact_count, fact_unique = cur.fetchone()

        print(
            f"{table}: staging={source_count}, fact={fact_count}, "
            f"ID fact unik={fact_unique}"
        )

        if source_count != fact_count:
            errors.append(
                f"{table}: jumlah fact ({fact_count}) berbeda dari staging ({source_count})"
            )
        if fact_count != fact_unique:
            errors.append(f"{table}: ditemukan ID fact duplikat")
        if source_count != source_unique:
            errors.append(f"{source_table}: ID sumber tidak unik")

    # 2. Foreign-key orphan check for fact tables
    print("\n[2] VALIDASI FOREIGN KEY FACT")
    cur.execute("""
        SELECT
            cs.name AS child_schema,
            ct.name AS child_table,
            cc.name AS child_column,
            ps.name AS parent_schema,
            pt.name AS parent_table,
            pc.name AS parent_column,
            fkc.constraint_object_id,
            fkc.constraint_column_id
        FROM sys.foreign_key_columns fkc
        JOIN sys.tables ct ON fkc.parent_object_id = ct.object_id
        JOIN sys.schemas cs ON ct.schema_id = cs.schema_id
        JOIN sys.columns cc
          ON cc.object_id = ct.object_id
         AND cc.column_id = fkc.parent_column_id
        JOIN sys.tables pt ON fkc.referenced_object_id = pt.object_id
        JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
        JOIN sys.columns pc
          ON pc.object_id = pt.object_id
         AND pc.column_id = fkc.referenced_column_id
        WHERE cs.name = 'fact'
        ORDER BY ct.name, fkc.constraint_object_id, fkc.constraint_column_id
    """)
    fk_rows = cur.fetchall()

    # Group multi-column foreign keys together.
    grouped = {}
    for row in fk_rows:
        child_schema, child_table, child_col, parent_schema, parent_table, parent_col, fk_id, ordinal = row
        key = (child_schema, child_table, parent_schema, parent_table, fk_id)
        grouped.setdefault(key, []).append((ordinal, child_col, parent_col))

    if not grouped:
        print("Tidak ditemukan constraint FOREIGN KEY pada schema fact.")
        print("Catatan: hasil ini berarti validasi orphan otomatis tidak dapat dijalankan.")
    else:
        for (cs, ct, ps, pt, fk_id), columns in grouped.items():
            columns.sort()
            join_condition = " AND ".join(
                f"c.[{child_col}] = p.[{parent_col}]"
                for _, child_col, parent_col in columns
            )
            null_condition = " AND ".join(
                f"p.[{parent_col}] IS NULL"
                for _, _, parent_col in columns
            )
            query = f"""
                SELECT COUNT(*)
                FROM [{cs}].[{ct}] c
                LEFT JOIN [{ps}].[{pt}] p ON {join_condition}
                WHERE {null_condition}
            """
            cur.execute(query)
            orphan_count = cur.fetchone()[0]
            print(f"{cs}.{ct} -> {ps}.{pt}: orphan={orphan_count}")
            if orphan_count:
                errors.append(
                    f"Foreign key {cs}.{ct} -> {ps}.{pt} memiliki {orphan_count} orphan"
                )

    # 3. Check that fact date keys map to the source row's date.
    print("\n[3] KECOCOKAN TANGGAL FACT DENGAN STAGING")
    date_checks = [
        (
            "FactEquipmentUsage",
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
            "FactProduction",
            """
            SELECT COUNT(*)
            FROM staging.Production s
            JOIN fact.FactProduction f ON f.production_id = s.production_id
            JOIN dim.DimTime t ON t.time_key = f.time_key
            WHERE CAST(s.[date] AS date) <> CAST(t.[date] AS date)
            """,
        ),
        (
            "FactFinancialTransaction",
            """
            SELECT COUNT(*)
            FROM staging.FinancialTransaction s
            JOIN fact.FactFinancialTransaction f ON f.transaction_id = s.id
            JOIN dim.DimTime t ON t.time_key = f.time_key
            WHERE CAST(s.[date] AS date) <> CAST(t.[date] AS date)
            """,
        ),
    ]
    for name, query in date_checks:
        cur.execute(query)
        mismatches = cur.fetchone()[0]
        print(f"{name}: tanggal tidak cocok={mismatches}")
        if mismatches:
            errors.append(f"{name}: {mismatches} tanggal fact tidak cocok dengan staging")

    # 4. Compare direct measures and totals against staging by source ID.
    print("\n[4] PEMBANDINGAN NILAI SUM DENGAN STAGING")
    for item in FACTS:
        table = item["table"]
        source_table = item["source_table"]
        fact_id = item["fact_id"]
        source_id = item["source_id"]
        for fact_measure, source_measure in item["measures"]:
            query = f"""
                SELECT
                    COALESCE(SUM(CAST(f.[{fact_measure}] AS decimal(38, 6))), 0),
                    COALESCE(SUM(CAST(s.[{source_measure}] AS decimal(38, 6))), 0),
                    COUNT(*)
                FROM {source_table} s
                JOIN {table} f ON f.[{fact_id}] = s.[{source_id}]
            """
            cur.execute(query)
            fact_sum, source_sum, matched_rows = cur.fetchone()
            same = fact_sum == source_sum
            print(
                f"{table}.{fact_measure}: fact_sum={fmt(fact_sum)}, "
                f"staging_sum={fmt(source_sum)}, matched={matched_rows}, "
                f"match={'YA' if same else 'TIDAK'}"
            )
            if not same:
                errors.append(
                    f"{table}.{fact_measure}: SUM fact tidak sama dengan staging"
                )

    print("\n" + "=" * 72)
    if errors:
        print(f"STATUS: PERLU PERBAIKAN ({len(errors)} temuan)")
        for i, error in enumerate(errors, 1):
            print(f"{i}. {error}")
    else:
        print("STATUS: SEMUA PEMERIKSAAN YANG TERJANGKAU LOLOS")
    print("=" * 72)

    cur.close()
    conn.close()

    # Exit status non-zero if validation found issues.
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
