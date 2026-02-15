import requests
from pathlib import Path
from typing import Dict, List, Optional


class EurostatFilterClient:
    """
    Eurostat SDMX 3.0 client using filter syntax (c[dimension]).
    """

    def __init__(self, dataset: str, base_url: str,
                 output_dir: str = "data/raw/eurostat",
                 agency: str = "ESTAT", version: str = "1.0"):
        self.dataset = dataset
        self.base_url = base_url
        self.agency = agency
        self.version = version
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch_xml(self, filters: Dict[str, List[str]], compress: bool = True,
                  filename: Optional[str] = None) -> Path:
        """
        Fetch the XML data from the eurostat API
        :param filters: API parameters,required:
            time (str)
            geo (str)
            freq (str)
        """

        url = (
            f"{self.base_url}/{self.agency}/{self.dataset}/"
            f"{self.version}/*.*.*.*"
        )

        params = {"compress": str(compress).lower()}

        for dim, values in filters.items():
            params[f"c[{dim}]"] = ",".join(values)

        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()

        if not filename:
            filename = f"{self.dataset}.xml"

        file_path = self.output_dir / filename

        with open(file_path, "wb") as f:
            f.write(response.content)

        return file_path
