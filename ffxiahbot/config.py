"""
The configuration for the bot.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from ffxiahbot.logutils import logger


class SellerPersona(BaseModel):
    """
    Seller persona config.
    """

    model_config = ConfigDict(extra="forbid")

    id: int
    name: str
    weight: float = Field(default=1.0, gt=0)


# noinspection PyArgumentList
class Config(BaseModel):
    """
    The configuration for the bot.
    """

    model_config = ConfigDict(extra="forbid")

    # Bot
    name: str = Field(default="M.H.M.U.")  # Bot name
    tick: int = Field(default=30)  # Tick interval (seconds)
    restock: int = Field(default=3600)  # Restock interval (seconds)
    use_buying_rates: bool = Field(default=False)  # Only buy items a fraction of the time?
    use_selling_rates: bool = Field(default=False)  # Only sell items a fraction of the time?
    seller_pool_path: Path | None = Field(default=Path("seller_personae.yaml"))  # Optional external seller pool file.
    seller_pool: list[SellerPersona] | None = Field(default=None)  # Optional pool of seller identities.
    use_seller_pool_weights: bool = Field(default=False)  # Use seller persona weights when picking listing sellers.
    sell_price_jitter_min_percent: float = Field(default=0.0, ge=0.0)  # Min listing price jitter percentage.
    sell_price_jitter_max_percent: float = Field(default=0.0, ge=0.0)  # Max listing price jitter percentage.
    sell_overstock_attempt_cap: int = Field(default=0, ge=0)  # Extra attempts after target stock is reached.
    sell_overstock_decay: float = Field(default=0.5, gt=0.0, le=1.0)  # Geometric decay for overstock attempts.
    buy_price_slippage_percent: float = Field(default=0.0, ge=0.0)  # Allowed buy-price slippage above configured cap.

    # Database
    hostname: str = Field(default="127.0.0.1")  # SQL address
    database: str = Field(default="xidb")  # SQL database
    username: str = Field(default="xi")  # SQL username
    password: SecretStr | str = Field(default=SecretStr("password"))  # SQL password
    port: int = Field(default=3306)  # SQL port
    fail: bool = Field(default=False)  # Fail on SQL errors?

    @classmethod
    def from_yaml(cls, cfg_path: Path | None = None) -> Config:
        """
        Load the configuration from a file.

        Args:
            cfg_path: The path to the configuration file.

        Returns:
            The configuration instance.
        """
        if cfg_path is None:
            return cls()

        with cfg_path.open("r") as stream:
            data = yaml.safe_load(stream) or {}
            for key in data:
                if key.lower() in DEPRECATED:
                    logger.error(f"Ignoring deprecated config key: {key}")

            filtered_data = {k: v for k, v in data.items() if k.lower() not in DEPRECATED}
            if filtered_data.get("seller_pool") is None:
                filtered_data["seller_pool"] = cls._load_external_seller_pool(
                    cfg_path=cfg_path,
                    seller_pool_path=filtered_data.get("seller_pool_path", Path("seller_personae.yaml")),
                )

            return cls(**filtered_data)

    @staticmethod
    def _load_external_seller_pool(cfg_path: Path, seller_pool_path: Path | str | None) -> list[dict] | None:
        """
        Load seller personas from a separate YAML file.

        Args:
            cfg_path: The main config path.
            seller_pool_path: Path to seller persona file.

        Returns:
            A seller pool payload compatible with Config.seller_pool, or None.
        """
        if seller_pool_path is None:
            return None

        pool_path = Path(seller_pool_path)
        if not pool_path.is_absolute():
            pool_path = cfg_path.parent / pool_path

        if not pool_path.exists():
            return None

        with pool_path.open("r") as stream:
            pool_data = yaml.safe_load(stream) or {}

        if isinstance(pool_data, list):
            return pool_data

        if isinstance(pool_data, dict):
            seller_pool = pool_data.get("seller_pool")
            if seller_pool is None:
                return None
            if not isinstance(seller_pool, list):
                raise TypeError("seller_pool must be a list")
            return seller_pool

        raise TypeError("seller_personae.yaml must contain a list or a seller_pool mapping")


DEPRECATED = {
    "data": "use --inp-csv",
    "overwrite": "use --overwrite",
    "backup": "use --backup",
    "stub": "use --out-csv",
    "server": "use --server",
    "threads": "use --threads",
    "stock_stacks": "use --default-stock-stack",
    "stock_single": "use --default-stock-single",
    "itemids": "use --item-ids",
    "urls": "use --urls",
    "verbose": "use --verbose",
    "silent": "use --silent",
}
