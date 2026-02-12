from common.http_client import get_xml

def fetch_dataset(dataset: str, base_url: str) -> str:
    url = f"{base_url}/{dataset}"
    return get_xml(url)

