import numpy as np
import pandas as pd
from prefect import task, get_run_logger

def verwijder_kolommen(df, kolommen):
    df = df.drop(columns=kolommen)
    return df

def waarde_naar_nan(df, kolom, waarde):
    df.loc[df[kolom] == waarde, kolom] = np.nan
    return df

def boven_grens_naar_nan(df, kolom, grens):
    df.loc[df[kolom] > grens, kolom] = np.nan
    return df

def jonge_moeder_naar_nan(df, min_leeftijd):

    fout = (df["lft"] < min_leeftijd) & (df["par"] >= 1)
    df.loc[fout, "lft"] = np.nan
    return df

def par_grav_naar_nan(df):

    fout = df["par"] >= df["grav"]
    df.loc[fout, ["par", "grav"]] = np.nan
    return df

def corrigeer_typefout(df, kolom, grens, factor):
    fout = df[kolom] > grens
    df.loc[fout, kolom] = df.loc[fout, kolom] / factor
    return df

def leid_beginbaring_af(df, sectio_primair, primaire_sectio):
    leeg = df["beginbaring_basis"].isna()
    is_sectio_primair = df["type_partus"] == sectio_primair

    df["beginbaring_afgeleid"] = leeg & is_sectio_primair
    df.loc[df["beginbaring_afgeleid"], "beginbaring_basis"] = primaire_sectio
    return df

def kolom_provincie_bekend(df, onbekend):
    df["provincie_bekend"] = df["provincie"] != onbekend
    return df

def inconsistent_thuis_sectio(df, thuis, sectio_codes):
    thuis_bevalling = df["gebplaats"] == thuis
    sectio = df["type_partus"].isin(sectio_codes)
    df["inconsistent"] = thuis_bevalling & sectio
    return df

def inconsistent_spontaan_primair(df, spontaan, sectio_primair):
    spontaan_begin = df["beginbaring_basis"] == spontaan
    primair = df["type_partus"] == sectio_primair
    df["inconsistent"] = df["inconsistent"] | (spontaan_begin & primair)
    return df

# code vervangen door de leesbare naam
def vervang_code_label(df, kolom, labels):
    df[kolom] = df[kolom].astype("Int64").map(labels)
    return df

#leeftijd omzetten naar leeftijdsgroep privacybydesign: abstract
def voeg_leeftijdsgroep_toe(df):
    grenzen = [0, 20, 25, 30, 35, 40, 200]
    namen = ["<20", "20-24", "25-29", "30-34", "35-39", "40+"]
    df["leeftijdsgroep"] = pd.cut(df["lft"], bins=grenzen, labels=namen, right=False)
    return df

def voeg_ligging_afwijkend_toe(df, normale_ligging):
    df["ligging_afwijkend"] = df["ligging"] != normale_ligging
    df["ligging_afwijkend"] = df["ligging_afwijkend"].astype("boolean")
    # als ligging onbekend is weet ik ook niet of hij afwijkend is
    df.loc[df["ligging"].isna(), "ligging_afwijkend"] = pd.NA
    return df

def controleer_toegestane_kolommen(df, toegestaan):
    for kolom in df.columns:
        if kolom not in toegestaan:
            raise ValueError("Kolom niet toegestaan: " + kolom)

def onderdruk_kleine_groepen(df, kolommen, minimum):
    # grootte van de groep op elke rij zetten
    grootte = df.groupby(kolommen)["jaar"].transform("size")
    klein = grootte < minimum
    df.loc[klein, "leeftijdsgroep"] = np.nan
    return df

@task
def transform(raw_pad, clean_pad, weg_kolommen, lft_min, par_max, grav_max, codes, labels, privacy):
    logger = get_run_logger()
    logger.info("Begin transform")
    df = pd.read_parquet(raw_pad)

    df = verwijder_kolommen(df, weg_kolommen)

    df = waarde_naar_nan(df, "geboortegew", 0)
    df = jonge_moeder_naar_nan(df, lft_min)
    df = boven_grens_naar_nan(df, "par", par_max)
    df = boven_grens_naar_nan(df, "grav", grav_max)
    df = par_grav_naar_nan(df)

    df = corrigeer_typefout(df, "geboortegew", 1_000_000, 1000)

    df = leid_beginbaring_af(df, codes["sectio_primair"], codes["primaire_sectio"])

    df = kolom_provincie_bekend(df, codes["provincie_onbekend"])
    df = inconsistent_thuis_sectio(df, codes["thuis"], codes["sectio"])
    df = inconsistent_spontaan_primair(df, codes["spontaan_begin"], codes["sectio_primair"])

    df = voeg_ligging_afwijkend_toe(df, codes["normale_ligging"])
    df = voeg_leeftijdsgroep_toe(df)
    for kolom in labels:
        df = vervang_code_label(df, kolom, labels[kolom])
    df = verwijder_kolommen(df, privacy["weg"])
    controleer_toegestane_kolommen(df, privacy["toegestaan"])
    df = onderdruk_kleine_groepen(df, privacy["herkenbaar"],5) #minder dan k = 5 
                                                         #kan gevaarlijk zijn
                                                         #dus leeftijdgroep
                                                         #verwijderen.

    df.to_parquet(clean_pad, index=False)
    logger.info("Transform klaar, aantal kolommen: %s", len(df.columns))
