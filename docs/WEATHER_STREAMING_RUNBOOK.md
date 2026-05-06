# Weather Streaming Runbook

## Objectif

Valider rapidement le lot meteo (ingestion + streaming) avant une demo ou un merge.

## Prerequis

- Environnement Python avec `pyspark` installe.
- Variables d'environnement chargees (`.env`).
- PostgreSQL demarre et table `dim_weather` creee.

## Etapes de verification

1. Verifier la configuration
   - `python -m scripts.processing.weather_streaming.smoke_checks`

2. Lancer une collecte meteo unique
   - `python -m scripts.ingestion.weather.fetch_weather --once`

3. Lancer le streaming en traitement unique
   - `python -m scripts.processing.weather_streaming.stream_weather --trigger-once`

4. Verifier les donnees en base
   - `SELECT event_ts, city, weather_category, temperature_c FROM dim_weather ORDER BY weather_id DESC LIMIT 10;`

## En cas d'erreur frequente

- `OPENWEATHER_API_KEY is required`:
  - verifier la variable `OPENWEATHER_API_KEY`.
- `No module named pyspark`:
  - installer `pyspark` dans l'environnement actif.
- Erreur JDBC PostgreSQL:
  - verifier `PGHOST`, `PGPORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
