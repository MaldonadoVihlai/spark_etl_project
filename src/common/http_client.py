import requests


def get_xml(url: str, params: dict | None = None) -> str:
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.text
