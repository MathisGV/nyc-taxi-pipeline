from scripts.ingestion.weather.config import load_weather_config, validate_weather_config


def main() -> None:
    config = load_weather_config()
    validate_weather_config(config)
    print(
        "weather config loaded "
        f"(city={config.openweather_city}, units={config.openweather_units}, "
        f"raw_path={config.weather_raw_path}, "
        f"poll_interval_s={config.weather_poll_interval_seconds})"
    )


if __name__ == "__main__":
    main()
