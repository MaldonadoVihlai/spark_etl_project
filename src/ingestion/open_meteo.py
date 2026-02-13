import requests
from typing import Any, Dict


class OpenMeteoClient:
    """
    Client for Open-Meteo API (forecast and archive endpoints).
    """

    BASE_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    BASE_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
    DEFAULT_LATITUDE = 4.624335
    DEFAULT_LONGITUDE = -74.063644
    TIME_ZONE = 'America/Bogota'
    VARIABLES = ["temperature_2m_max", "temperature_2m_min",
                 "relative_humidity_2m", "precipitation", "precipitation_sum"]

    def __init__(self, timeout: int = 30, max_retries: int = 3,
                 backoff_factor: float = 1.5) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def _request(self, url: str, params: Dict[str, Any]):
        attempt = 0

        while attempt < self.max_retries:
            try:
                print(f"Requesting {url} with params={params}")

                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                )

                response.raise_for_status()

                return response.json()

            except requests.RequestException as e:
                attempt += 1

                if attempt >= self.max_retries:
                    print(e)
                    raise

    def fetch_archive(self, **kwargs: Any):
        """
        Fetch historical weather data.
        :param kwargs: API parameters,required:
            latitude (float)
            longitude (float)
            start_date (YYYY-MM-DD)
            end_date (YYYY-MM-DD)
        Optional:
            daily (comma-separated str)
            timezone (str)
        """

        if "latitude" not in kwargs:
            kwargs["latitude"] = self.DEFAULT_LATITUDE
        if "longitude" not in kwargs:
            kwargs["longitude"] = self.DEFAULT_LONGITUDE
        if "timezone" not in kwargs:
            kwargs["timezone"] = self.TIME_ZONE

        required = ["latitude", "longitude", "start_date", "end_date"]
        for key in required:
            if key not in kwargs:
                raise ValueError(f"Missing required parameter: {key}")

        return self._request(self.BASE_ARCHIVE_URL, kwargs)

    def fetch_forecast(self, **kwargs: Any):
        """
        Fetch forecast weather data.
        :param kwargs: API parameters,required:
            latitude (float)
            longitude (float)
        Optional:
            hourly (comma-separated str)
            daily (comma-separated str)
            timezone (str)
        """

        if "latitude" not in kwargs:
            kwargs["latitude"] = self.DEFAULT_LATITUDE
        if "longitude" not in kwargs:
            kwargs["longitude"] = self.DEFAULT_LONGITUDE

        return self._request(self.BASE_FORECAST_URL, kwargs)
