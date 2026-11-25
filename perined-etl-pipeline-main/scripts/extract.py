import pandas as pd
from prefect import task, get_run_logger

def csv_inlezen(pad):
    return pd.read_csv(pad)


def controleer_kolommen(df, kolommen):
    for kolom in kolommen:
        if kolom not in df.columns:
            raise ValueError("Kolom ontbreekt: " + kolom)

def opslaan(df, pad):
    df.to_parquet(pad, index=False)

@task(retries=2, retry_delay_seconds=5)
def extract(bronbestand, raw_pad, kolommen):
    logger = get_run_logger()
    logger.info("Start extract")



    df = csv_inlezen(bronbestand)
    controleer_kolommen(df, kolommen)
    opslaan(df, raw_pad)

    logger.info("Extract klaar, aantal rijen: %s", len(df))
