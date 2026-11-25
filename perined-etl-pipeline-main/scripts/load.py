import os
import duckdb
from prefect import task, get_run_logger

def duckdb_inladen (conn, clean_pad, tabel):
    conn.execute("CREATE OR REPLACE TABLE " + tabel + " AS SELECT * FROM read_parquet('" + clean_pad + "')")

def rijen_tellen_tabel(conn, tabel):
    aantal = conn.execute("SELECT COUNT(*) FROM " + tabel).fetchone()[0]
    return aantal

def rijen_tellen_parquet(conn, pad):
    aantal = conn.execute("SELECT COUNT(*) FROM read_parquet('" + pad + "')").fetchone()[0]
    return aantal

def controleer_aantal_rijen(aantal_tabel, aantal_parquet):
    if aantal_tabel != aantal_parquet:
        raise ValueError("Rijen kloppen niet")

def verwijder_bestand(pad): # ruwe data weggoeien voor privacy
    if os.path.exists(pad):
        os.remove(pad)

@task
def load(clean_pad, database, tabel, raw_pad):
    logger = get_run_logger()
    logger.info("Begin load")
    conn = duckdb.connect(database)
    duckdb_inladen(conn, clean_pad, tabel)
    aantal_tabel = rijen_tellen_tabel(conn, tabel)
    aantal_parquet = rijen_tellen_parquet(conn, clean_pad)
    conn.close()
    controleer_aantal_rijen(aantal_tabel, aantal_parquet)
    logger.info("Tabel bevat %s rijen", aantal_tabel)
    verwijder_bestand(raw_pad)
    logger.info("raw.parquet verwijderd")
