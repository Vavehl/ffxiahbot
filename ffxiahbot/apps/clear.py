"""
Clear the auction house.
"""

from pathlib import Path
from typing import Annotated

from typer import Option, confirm

from ffxiahbot.common import OptionalPath
from ffxiahbot.config import Config
from ffxiahbot.database import Database
from ffxiahbot.logutils import logger
from ffxiahbot.tables.base import Base


def main(
    cfg_path: Annotated[Path, Option("--config", help="Config file path.")] = Path("config.yaml"),
    no_prompt: Annotated[bool, Option("--no-prompt", help="Do not ask for confirmation.")] = False,
    clear_all: Annotated[bool, Option("--all", help="Clear all items.")] = False,
    use_sqlite_db: Annotated[OptionalPath, Option(help="Use a test SQLite database instead of the real one?")] = None,
) -> None:
    """
    Delete items from the auction house ([red]dangerous operation![/]).
    """
    from ffxiahbot.auction.manager import Manager

    config: Config = Config.from_yaml(cfg_path)
    logger.info("%s", config.model_dump_json(indent=2))

    # create auction house manager
    if use_sqlite_db:
        manager = Manager.from_db(
            db=Database.sqlite(database=str(use_sqlite_db)),
            name=config.name,
            fail=config.fail,
            seller_pool=config.seller_pool,
            sell_price_jitter_min_percent=config.sell_price_jitter_min_percent,
            sell_price_jitter_max_percent=config.sell_price_jitter_max_percent,
            sell_overstock_attempt_cap=config.sell_overstock_attempt_cap,
            sell_overstock_decay=config.sell_overstock_decay,
            buy_price_slippage_percent=config.buy_price_slippage_percent,
            use_seller_pool_weights=config.use_seller_pool_weights,
        )
        Base.metadata.create_all(manager.db.engine)
    else:
        manager = Manager.create_database_and_manager(
            hostname=config.hostname,
            database=config.database,
            username=config.username,
            password=config.password,
            port=config.port,
            name=config.name,
            fail=config.fail,
            seller_pool=config.seller_pool,
            sell_price_jitter_min_percent=config.sell_price_jitter_min_percent,
            sell_price_jitter_max_percent=config.sell_price_jitter_max_percent,
            sell_overstock_attempt_cap=config.sell_overstock_attempt_cap,
            sell_overstock_decay=config.sell_overstock_decay,
            buy_price_slippage_percent=config.buy_price_slippage_percent,
            use_seller_pool_weights=config.use_seller_pool_weights,
        )

    # clear all items
    if clear_all:
        # really?
        if total := manager.cleaner.count(seller=None):
            if no_prompt or confirm(f"Clear all items? ({total} rows will be removed)", abort=True, show_default=True):
                manager.cleaner.clear(seller=None)
        else:
            logger.info("No items to clear.")
    # clear seller items
    else:
        if total := manager.cleaner.count(seller=manager.seller.seller):
            if no_prompt or confirm(
                f"Clear {config.name}'s items? ({total} rows will be removed)", abort=True, show_default=True
            ):
                manager.cleaner.clear(seller=manager.seller.seller)
        else:
            logger.info("No items to clear.")
