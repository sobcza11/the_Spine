from pathlib import Path
import duckdb

# Base paths
base = Path("data/processed")
econ_dir = base / "p_Econ_US"
infl_dir = base / "p_Inflation_US"
com_dir = base / "p_Com_US"

# Ensure directories exist
for d in [econ_dir, infl_dir, com_dir]:
    d.mkdir(parents=True, exist_ok=True)

# Connect to an in-memory DuckDB instance
con = duckdb.connect()

# 1) Placeholder econ_leaf.parquet
econ_path = (econ_dir / "econ_leaf.parquet").as_posix()
con.execute(
    f"""
    COPY (
        SELECT
            DATE '2025-01-01' AS date,
            0.0::DOUBLE      AS econ_score
    ) TO '{econ_path}' (FORMAT 'parquet');
    """
)

# 2) Placeholder inflation_leaf.parquet
infl_path = (infl_dir / "inflation_leaf.parquet").as_posix()
con.execute(
    f"""
    COPY (
        SELECT
            DATE '2025-01-01' AS date,
            0.0::DOUBLE      AS inflation_score
    ) TO '{infl_path}' (FORMAT 'parquet');
    """
)

# 3) Placeholder wti_pressure_leaf.parquet
wti_path = (com_dir / "wti_pressure_leaf.parquet").as_posix()
con.execute(
    f"""
    COPY (
        SELECT
            DATE '2025-01-01' AS date,
            0.0::DOUBLE      AS wti_pressure_score
    ) TO '{wti_path}' (FORMAT 'parquet');
    """
)

print("Placeholder macro leaves created:")
print(f" - {econ_path}")
print(f" - {infl_path}")
print(f" - {wti_path}")

