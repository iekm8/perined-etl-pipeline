# Perined ETL

Dit is mijn ETL voor de synthetische Perined data. De ETL leest de data in, maakt de data schoon, maakt de data privacyvriendelijk en zet het resultaat in een DuckDB database. Ik gebruik Prefect om de ETL te draaien.

Gemaakt door Ismael El Khadmaoui Mahir (1122620).

## Deelvraag

In hoeverre verschilt de wijze van bevalling bij een afwijkende ligging tussen leeftijdsgroepen van de moeder?

De ETL maakt de data klaar om deze vraag te beantwoorden. Daarvoor maak ik onder andere de kolommen ligging_afwijkend en leeftijdsgroep.

## Mappen

- scripts: de code van de ETL
  - main.py: de flow en alle instellingen
  - extract.py: csv inlezen en opslaan als raw.parquet
  - transform.py: data schoonmaken en privacy stappen
  - load.py: data in DuckDB zetten
- eda: mijn EDA notebook (eda_ismael.ipynb)
- data: de data, deze map staat niet in Git
- requirements.txt: alle packages met hun versie

## Installeren

Ik gebruik Python 3.14. De versies van alle packages staan in requirements.txt.

    python3 -m venv etl-venv
    source etl-venv/bin/activate
    pip install -r requirements.txt

Zet daarna het bestand etl-synthetic_perined.csv in de map data.

## Draaien

Eerst de Prefect server starten in een terminal:

    prefect server start --host 0.0.0.0 --port 8888

Dan in een tweede terminal de ETL starten. Dit moet vanuit de map scripts, omdat de paden relatief zijn:

    cd scripts
    python3 main.py

In de Prefect UI (poort 8888) zie je of extract, transform en load gelukt zijn.

Na het draaien staan in de map data:
- clean.parquet
- perined.duckdb met de tabel perined

## Extract

- De csv inlezen.
- Controleren of alle 47 kolommen er zijn. Als er een kolom mist stopt de ETL.
- Opslaan als raw.parquet.
- Als het inlezen mislukt probeert Prefect het nog 2 keer.

## Transform

Alle keuzes komen uit mijn EDA. Ik verwijder geen rijen. Foute waarden maak ik leeg (NaN), zodat de rest van de bevalling bruikbaar blijft.

1. Kolommen verwijderen die helemaal leeg zijn of overal dezelfde waarde hebben.
2. Onmogelijke waarden leeg maken: geboortegewicht 0, moeder jonger dan 14 die al eerder bevallen is, par boven 15, grav boven 20 en par groter of gelijk aan grav.
3. Typefout in geboortegewicht herstellen. 3.694.000 gram wordt 3.694 gram.
4. beginbaring_basis invullen als type_partus een primaire sectio is. De vlag beginbaring_afgeleid laat zien welke waarden ik heb ingevuld.
5. Vlag provincie_bekend maken, omdat 37,6% een onbekende provincie heeft (code 99).
6. Vlag inconsistent maken voor thuis met sectio en spontaan begin met sectio primair. Ik weet niet welke kolom fout is, dus ik markeer alleen de rij.
7. ligging_afwijkend en leeftijdsgroep maken en de codes vervangen door namen uit het Codeboek.
8. lft en pc4 verwijderen.
9. Controleren dat alleen toegestane kolommen in de output staan.
10. Kleine groepen onderdrukken.

Het resultaat heeft 100.000 rijen en 41 kolommen.

## Load

- clean.parquet in DuckDB zetten.
- Controleren of het aantal rijen in de tabel gelijk is aan clean.parquet. Als het niet klopt stopt de ETL.
- raw.parquet verwijderen.

## Datakwaliteit

Ik gebruik de datakwaliteitsdimensies van DAMA.
- Completeness: lege kolommen verwijderen en de vlag provincie_bekend.
- Validity en reasonableness: onmogelijke waarden leeg maken.
- Accuracy: de typefout in geboortegewicht herstellen.
- Consistency: par en grav controleren en de vlag inconsistent.

Ontbrekende waarden heb ik in de EDA gecontroleerd. lft, ligging, type_partus en geboortegewicht zijn MCAR. Die vul ik niet in. beginbaring_basis is MAR (hangt af van gebplaats). Daar vul ik alleen in wat zeker is.

## Privacy by design

- Minimise: pc4 en lft worden verwijderd. Alleen kolommen op de lijst toegestaan mogen in de output, anders stopt de ETL.
- Abstract: de leeftijd wordt een leeftijdsgroep. Als een groep van provincie, jaar en leeftijdsgroep kleiner is dan 5 mensen, wordt de leeftijdsgroep leeg gemaakt. In deze dataset waren dat 184 rijen.
- Enforce: raw.parquet wordt na het laden automatisch verwijderd, want daar staan lft en pc4 nog in.
- Demonstrate: de logs laten alleen aantallen zien, nooit gegevens van personen. Afgeleide en foute waarden zijn te zien aan de vlaggen.

De map data staat niet in Git.

## Versiebeheer

- Ik werk in branches en push nooit direct naar main.
- Wijzigingen komen via een merge request in main.
- De data voeg ik niet toe aan Git.

## Echte data

De ETL is gemaakt om later ook met echte data te werken. Pas daarvoor in main.py het bronbestand aan. Controleer dan ook of alle codes in de labels staan, want een onbekende code wordt leeg. Vraag ook na of een minimum van 5 personen per groep genoeg is.
