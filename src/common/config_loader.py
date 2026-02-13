import yaml
from pathlib import Path
from common.exceptions import ConfigError


def load_config(path: str | Path) -> dict:
    path = Path(path)

    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ConfigError("Invalid config format")

    return config
