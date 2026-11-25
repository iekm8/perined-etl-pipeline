from prefect import flow, get_run_logger
from extract import extract
from transform import transform
from load import load

bronbestand = "../data/etl-synthetic_perined.csv"
raw_pad = "../data/raw.parquet"
clean_pad = "../data/clean.parquet"
database = "../data/perined.duckdb"
tabel = "perined"

kolommen = [
    "ovdr12", "verantw_zw", "verantw_bb", "verantw_eb", "jaar", "pc4", "provincie",
    "gebplaats", "type_partus", "beginbaring_detail", "lft", "ses_percentiel", "par",
    "grav", "gesl", "geboortegew", "apgar5", "amddd", "hoftiezer", "amww", "whodef",
    "amddd1ond", "ligging", "pediater", "fluxus", "congenafw1", "congenafw2",
    "congenafw3", "congenafw4", "congenafw5", "congenafw6", "congenafw7", "congenafw8",
    "sterfte", "stedelijkheid", "pijnbestrijding2", "nicuopname", "versienummer",
    "alcoholgebruik", "drugsgebruik", "episiotomie", "ruptuur", "vrouwniveau",
    "beginbaring_basis", "etnic", "rookgedrag", "sectio_ia",
]

weg_kolommen = ["congenafw1", "congenafw6", "congenafw7", "congenafw8",
                "etnic", "rookgedrag", "sectio_ia", "whodef", "vrouwniveau"]


privacy = {
    "weg": ["lft", "pc4"],
    "herkenbaar": ["provincie", "jaar", "leeftijdsgroep"],
    "toegestaan": [
        "ovdr12", "verantw_zw", "verantw_bb", "verantw_eb", "jaar", "provincie",
        "gebplaats", "type_partus", "beginbaring_detail", "ses_percentiel", "par",
        "grav", "gesl", "geboortegew", "apgar5", "amddd", "hoftiezer", "amww",
        "amddd1ond", "ligging", "pediater", "fluxus", "congenafw2", "congenafw3",
        "congenafw4", "congenafw5", "sterfte", "stedelijkheid", "pijnbestrijding2",
        "nicuopname", "versienummer", "alcoholgebruik", "drugsgebruik", "episiotomie",
        "ruptuur", "beginbaring_basis",
        "beginbaring_afgeleid", "provincie_bekend", "inconsistent",
        "leeftijdsgroep", "ligging_afwijkend",
    ],
}

codes = {
    "sectio_primair": 200148001,
    "sectio": [200148001, 200149009],
    "primaire_sectio": 2,
    "spontaan_begin": 3,
    "thuis": 0,
    "provincie_onbekend": 99,
    "normale_ligging": 70028003,
}

labels = {
    "ligging": {70028003: "Achterhoofd-voor", 37235006: "Achterhoofd-achter",
                249079005: "Kruin", 21882006: "Aangezicht", 8014007: "Voorhoofd",
                49168004: "Volkomen stuit", 38049006: "Half onvolkomen stuit",
                18559007: "Onvolkomen stuit"},
    "type_partus": {48782003: "Spontaan", 8561000146109: "Vaginale kunstverlossing",
                    200148001: "Sectio primair", 200149009: "Sectio secundair",
                    39406005: "TOP"},
    "beginbaring_basis": {1: "Inleiding", 2: "Primaire sectio", 3: "Spontaan", 9: "Onbekend"},
    "gebplaats": {0: "Thuis", 1: "Geboortecentrum", 2: "Ziekenhuis 1e lijn",
                  3: "Ziekenhuis 2e lijn", 4: "Onderweg", 5: "Onbekend"},
}



@flow
def perined_etl():
    logger = get_run_logger()
    logger.info("begin etl")

    extract(bronbestand, raw_pad, kolommen)
    transform(raw_pad, clean_pad, weg_kolommen, 14, 15, 20, codes, labels, privacy)
    load(clean_pad, database, tabel, raw_pad)








if __name__ == "__main__":
    perined_etl()
