# Runbook Weather Pipeline

## Objectif

Valider rapidement le lot meteo (ingestion + transformation) avant une demo ou un merge. (Pour éviter la sempiternelle remarque du "ça marche chez moi")

## Prérequis

- Environnement Python avec `pyspark` installe.
- Variables d'environnement chargees (`.env`).
- PostgreSQL demarre et table `raw.dim_weather` creee.

## Etapes de vérification

1. Vérifier la configuration MinIO/PostgreSQL
   - vérifier `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
   - vérifier `PGHOST`, `PGPORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

2. Lancer une collecte meteo unique
   - `python scripts/ingestion/ingest_weather.py --once`

3. Lancer la transformation meteo pour une heure donnee
   - `spark-submit scripts/processing/transform_weather.py --date 2026-05-06 --hour 10`

4. Vérifier les donnees en base
   - `SELECT recorded_at, temperature, weather_category, pickup_hour FROM raw.dim_weather ORDER BY weather_id DESC LIMIT 10;`

## En cas d'erreur fréquente

- `OPENWEATHER_API_KEY is required`:
  - vérifier la variable `OPENWEATHER_API_KEY`.
- `No module named pyspark`:
  - installer `pyspark` dans l'environnement actif.
- Erreur S3A/MinIO:
  - vérifier `MINIO_S3_ENDPOINT` (Spark) et `MINIO_ENDPOINT` (scripts Python).
- Erreur JDBC PostgreSQL:
  - vérifier `PGHOST`, `PGPORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
