from __future__ import annotations

import datetime
import random
from collections import Counter
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pydantic import SecretStr

from ffxiahbot import timeutils
from ffxiahbot.auction.browser import Browser
from ffxiahbot.auction.buyer import Buyer
from ffxiahbot.auction.cleaner import Cleaner
from ffxiahbot.auction.seller import Seller
from ffxiahbot.auction.worker import Worker
from ffxiahbot.common import progress_bar
from ffxiahbot.config import SellerPersona as ConfigSellerPersona
from ffxiahbot.database import Database
from ffxiahbot.itemlist import ItemList
from ffxiahbot.logutils import capture, logger
from ffxiahbot.tables.auctionhouse import AuctionHouse


@dataclass(frozen=True)
class SellerPersona:
    """
    Seller identity data for a listing persona.
    """

    seller: int
    seller_name: str
    weight: float = 1.0


@dataclass(frozen=True)
class Manager(Worker):
    """
    Auction House browser.
    """

    #: Auction House database row ids to ignore.
    blacklist: set[int]
    #: A worker to browse the auction house.
    browser: Browser
    #: A worker to clean the auction house.
    cleaner: Cleaner
    #: A worker to sell and restock items.
    seller: Seller
    #: A worker to purchase items.
    buyer: Buyer
    #: Pool of possible seller personas.
    seller_pool: tuple[SellerPersona, ...]
    #: Minimum percentage jitter for listing prices.
    sell_price_jitter_min_percent: float
    #: Maximum percentage jitter for listing prices.
    sell_price_jitter_max_percent: float
    #: Maximum number of extra overstock listing attempts per restock cycle.
    sell_overstock_attempt_cap: int
    #: Geometric decay factor for each successive overstock attempt.
    sell_overstock_decay: float
    #: Additional percentage allowed above configured buy caps.
    buy_price_slippage_percent: float
    #: If True, use configured seller persona weights when selecting sellers.
    use_seller_pool_weights: bool

    @classmethod
    def create_database_and_manager(
        cls,
        hostname: str,
        database: str,
        username: str,
        password: str | SecretStr,
        port: int,
        **kwargs: Any,
    ) -> Manager:
        """
        Create database and manager at the same time.

        Args:
            hostname: Database hostname.
            database: Database name.
            username: Database username.
            password: Database password.
            port: Database port.
            **kwargs: Additional arguments passed to the `Manager.from_db` method.
        """
        # connect to database
        db = Database.pymysql(
            hostname=hostname,
            database=database,
            username=username,
            password=password if isinstance(password, str) else password.get_secret_value(),
            port=port,
        )

        # create auction house manager
        return cls.from_db(db=db, **kwargs)

    @classmethod
    def from_db(
        cls,
        db: Database,
        name: str,
        fail: bool = True,
        rollback: bool = True,
        blacklist: set[int] | None = None,
        seller_pool: list[ConfigSellerPersona] | list[SellerPersona] | None = None,
        sell_price_jitter_min_percent: float = 0.0,
        sell_price_jitter_max_percent: float = 0.0,
        sell_overstock_attempt_cap: int = 0,
        sell_overstock_decay: float = 0.5,
        buy_price_slippage_percent: float = 0.0,
        use_seller_pool_weights: bool = False,
    ) -> Manager:
        """
        Create manager from database.

        Args:
            db: Database connection.
            name: Name of the seller and buyer.
            fail: If True, raise exceptions.
            rollback: If True, rollback transactions.
            blacklist: Auction House row ids to ignore.
            seller_pool: Optional pool of seller personas for restocking.
            sell_price_jitter_min_percent: Minimum absolute percent jitter for listing prices.
            sell_price_jitter_max_percent: Maximum absolute percent jitter for listing prices.
            sell_overstock_attempt_cap: Maximum number of additional listings to attempt once stock is reached.
            sell_overstock_decay: Geometric decay factor for each additional overstock attempt.
            buy_price_slippage_percent: Allowed percent above configured buy cap before rejecting purchase.
            use_seller_pool_weights: If True, honor configured seller_pool weights for persona selection.
        """
        if sell_price_jitter_min_percent > sell_price_jitter_max_percent:
            raise ValueError("sell_price_jitter_min_percent cannot exceed sell_price_jitter_max_percent")
        if sell_overstock_attempt_cap < 0:
            raise ValueError("sell_overstock_attempt_cap cannot be negative")
        if not 0 < sell_overstock_decay <= 1:
            raise ValueError("sell_overstock_decay must be within (0, 1]")
        if buy_price_slippage_percent < 0:
            raise ValueError("buy_price_slippage_percent cannot be negative")

        personas = cls._normalize_seller_pool(name=name, seller_pool=seller_pool)
        primary = personas[0]
        return cls(
            db=db,
            blacklist=blacklist if blacklist is not None else set(),
            browser=Browser(db=db, rollback=rollback, fail=fail),
            cleaner=Cleaner(db=db, rollback=rollback, fail=fail),
            seller=Seller(db=db, rollback=rollback, fail=fail, seller=primary.seller, seller_name=primary.seller_name),
            buyer=Buyer(db=db, rollback=rollback, fail=fail, buyer_name=name),
            seller_pool=tuple(personas),
            sell_price_jitter_min_percent=sell_price_jitter_min_percent,
            sell_price_jitter_max_percent=sell_price_jitter_max_percent,
            sell_overstock_attempt_cap=sell_overstock_attempt_cap,
            sell_overstock_decay=sell_overstock_decay,
            buy_price_slippage_percent=buy_price_slippage_percent,
            use_seller_pool_weights=use_seller_pool_weights,
            rollback=rollback,
            fail=fail,
        )

    @staticmethod
    def _normalize_seller_pool(
        name: str,
        seller_pool: list[ConfigSellerPersona] | list[SellerPersona] | None,
    ) -> list[SellerPersona]:
        if not seller_pool:
            return [SellerPersona(seller=0, seller_name=name, weight=1.0)]

        normalized: list[SellerPersona] = []
        for persona in seller_pool:
            if isinstance(persona, ConfigSellerPersona):
                normalized.append(SellerPersona(seller=persona.id, seller_name=persona.name, weight=persona.weight))
            else:
                normalized.append(persona)
        return normalized

    def _choose_seller_persona(self) -> SellerPersona:
        """
        Choose a seller persona for the current listing attempt.
        """
        weights = [p.weight for p in self.seller_pool] if self.use_seller_pool_weights else None
        return random.choices(self.seller_pool, weights=weights, k=1)[0]

    def add_to_blacklist(self, rowid: int) -> None:
        """
        Add row to blacklist.

        Args:
            rowid: Row id to blacklist.
        """
        logger.info("blacklisting: row=%d", rowid)
        self.blacklist.add(rowid)

    def buy_items(self, item_list: ItemList, use_buying_rates: bool = False) -> None:  # noqa: C901
        """
        The main buy item loop.

        Args:
            item_list: Item data.
            use_buying_rates: If True, only buy items at their buying rate.
        """
        with self.scoped_session(fail=self.fail) as session:
            # find rows that are still up for sale
            q = session.query(AuctionHouse).filter(
                AuctionHouse.seller != self.seller.seller,
                AuctionHouse.sell_date == 0,
                AuctionHouse.sale == 0,
            )
            # loop rows
            counts: Counter[str] = Counter()
            for row in q:
                if row.id in self.blacklist:
                    logger.debug("skipping blacklisted row %d", row.id)
                    counts["blacklisted"] += 1
                    continue

                if row.itemid not in item_list:
                    logger.error("item missing from database: %d", row.itemid)
                    self.add_to_blacklist(row.id)
                    counts["unknown itemid"] += 1
                    continue

                with capture(fail=self.fail):
                    item = item_list[row.itemid]

                    if row.stack:
                        if not item.buy_stacks:
                            logger.debug("buy skip forbidden stack item: row=%d itemid=%d", row.id, row.itemid)
                            self.add_to_blacklist(row.id)
                            counts["forbidden item"] += 1
                        else:
                            if not use_buying_rates:
                                gate_roll = None
                                passed_rate_gate = True
                            else:
                                gate_roll = random.random()
                                passed_rate_gate = gate_roll <= item.buy_rate_stacks

                            if passed_rate_gate:
                                if self._buy_row(row, item.price_stacks):
                                    counts["stack purchased"] += 1
                                else:
                                    counts["price too high"] += 1
                            else:
                                logger.debug(
                                    "buy skip rate gate failed: row=%d itemid=%d stack=1 roll=%.4f required<=%.4f",
                                    row.id,
                                    row.itemid,
                                    gate_roll,
                                    item.buy_rate_stacks,
                                )
                                counts["buy rate too low"] += 1
                    else:
                        if not item.buy_single:
                            logger.debug("buy skip forbidden single item: row=%d itemid=%d", row.id, row.itemid)
                            self.add_to_blacklist(row.id)
                            counts["forbidden item"] += 1
                        else:
                            if not use_buying_rates:
                                gate_roll = None
                                passed_rate_gate = True
                            else:
                                gate_roll = random.random()
                                passed_rate_gate = gate_roll <= item.buy_rate_single

                            if passed_rate_gate:
                                if self._buy_row(row, item.price_single):
                                    counts["single purchased"] += 1
                                else:
                                    counts["price too high"] += 1
                            else:
                                logger.debug(
                                    "buy skip rate gate failed: row=%d itemid=%d stack=0 roll=%.4f required<=%.4f",
                                    row.id,
                                    row.itemid,
                                    gate_roll,
                                    item.buy_rate_single,
                                )
                                counts["buy rate too low"] += 1

            counts_frame = pd.DataFrame.from_dict(counts, orient="index").rename(columns={0: "count"})
            if not counts_frame.empty:
                logger.debug("manager.buy_items counts: \n%s", counts_frame)
            else:
                logger.warning("no rows processed when buying items (no items for sale?)")

    def _buy_row(self, row: AuctionHouse, max_price: int) -> bool:
        """
        Buy a row.

        Args:
            row: Auction House row to buy.
            max_price: Maximum price to pay.
        """
        if max_price <= 0:
            logger.error("max buying price is zero! itemid=%d", row.itemid)
            self.add_to_blacklist(row.id)
            return False

        effective_max_price = int(round(max_price * (1 + (self.buy_price_slippage_percent / 100.0))))

        # check price
        if row.price <= effective_max_price:
            date = timeutils.timestamp(datetime.datetime.now())
            executed_price = AuctionHouse.validate_price(row.price)
            logger.debug(
                "buy execute: row=%d itemid=%d listed_price=%d configured_cap=%d effective_cap=%d slippage_pct=%.2f",
                row.id,
                row.itemid,
                row.price,
                max_price,
                effective_max_price,
                self.buy_price_slippage_percent,
            )
            self.buyer.set_row_buyer_info(row, date, executed_price)
            return True

        logger.info(
            "price too high: row=%d itemid=%d listed_price=%d configured_cap=%d effective_cap=%d slippage_pct=%.2f",
            row.id,
            row.itemid,
            row.price,
            max_price,
            effective_max_price,
            self.buy_price_slippage_percent,
        )
        self.add_to_blacklist(row.id)
        return False

    def restock_items(self, item_list: ItemList, use_selling_rates: bool = False) -> None:
        """
        The main restock loop.

        Args:
            item_list: Item data.
            use_selling_rates: If True, only restock items at their selling rate.
        """
        # loop over items
        with progress_bar("[red]Restocking Items...", total=len(item_list)) as (progress, task):
            for item in item_list.items.values():
                # singles
                if item.sell_single and item.price_single > 0:
                    self._sell_item(
                        item.itemid,
                        stack=False,
                        price=item.price_single,
                        stock=item.stock_single,
                        rate=None if not use_selling_rates else item.sell_rate_single,
                    )
                    progress.update(task, advance=0.5)

                # stacks
                if item.sell_stacks and item.price_stacks > 0:
                    self._sell_item(
                        item.itemid,
                        stack=True,
                        price=item.price_stacks,
                        stock=item.stock_stacks,
                        rate=None if not use_selling_rates else item.sell_rate_stacks,
                    )
                    progress.update(task, advance=0.5)

    @property
    def _sell_time(self) -> int:
        """
        The timestamp to use for selling items.
        """
        return timeutils.timestamp(datetime.datetime(2099, 1, 1))

    def _pool_stock(self, itemid: int, stack: bool) -> int:
        """
        Total stock for this manager's seller personas.
        """
        return sum(self.browser.get_stock(itemid=itemid, stack=stack, seller=persona.seller) for persona in self.seller_pool)

    def _pool_has_history(self, itemid: int, stack: bool) -> bool:
        """
        True if any seller persona already has historical pricing data.
        """
        return any(self.browser.get_price(itemid=itemid, stack=stack, seller=persona.seller) for persona in self.seller_pool)

    @staticmethod
    def _effective_sell_rate(rate: float, current_stock: int, target_stock: int) -> float:
        """
        Effective sell probability adjusted by local stock pressure.

        Lower relative stock increases probability, while higher stock pressure reduces it.
        """
        bounded_rate = max(0.0, min(1.0, rate))
        if target_stock <= 0:
            return bounded_rate

        stock_ratio = current_stock / target_stock
        pressure_factor = max(0.0, 2.0 - stock_ratio)
        return max(0.0, min(1.0, bounded_rate * pressure_factor))

    def _sell_item(self, itemid: int, stack: bool, price: int, stock: int, rate: float | None) -> None:
        """
        Sell an item.

        Args:
            itemid: The item id.
            stack: True if selling stacks, False if selling singles.
            price: The price to sell the item for.
            stock: The amount of items to stock.
        """
        counts: Counter[str] = Counter()

        # check history
        if not self._pool_has_history(itemid=itemid, stack=stack):
            persona = self._choose_seller_persona()
            self.seller.set_history(
                itemid=itemid,
                stack=stack,
                price=price,
                date=self._sell_time,
                count=1,
                seller=persona.seller,
                seller_name=persona.seller_name,
            )
            counts["history seeded"] += 1

        # restock
        current_stock = self._pool_stock(itemid=itemid, stack=stack)
        if current_stock < stock:
            for _ in range(stock - current_stock):
                probability = 1.0 if rate is None else self._effective_sell_rate(rate, current_stock, stock)
                roll = random.random()
                if roll <= probability:
                    persona = self._choose_seller_persona()
                    listing_price = self._sample_listing_price(base_price=price, stack=stack)
                    self.seller.sell_item(
                        itemid=itemid,
                        stack=stack,
                        date=self._sell_time,
                        price=listing_price,
                        count=1,
                        seller=persona.seller,
                        seller_name=persona.seller_name,
                    )
                    current_stock += 1
                    counts["restock sold"] += 1
                else:
                    logger.debug(
                        "sell skip rate gate failed: itemid=%d stack=%d roll=%.4f required<=%.4f",
                        itemid,
                        int(stack),
                        roll,
                        probability,
                    )
                    counts["restock rate gate failed"] += 1

        # optional overstock after target stock is reached
        if rate is None or self.sell_overstock_attempt_cap == 0:
            if counts:
                logger.debug("manager._sell_item counts itemid=%d stack=%d: %s", itemid, int(stack), dict(counts))
            return

        for attempt in range(self.sell_overstock_attempt_cap):
            effective_rate = self._effective_sell_rate(rate, current_stock, stock)
            probability = effective_rate * (self.sell_overstock_decay**attempt)
            roll = random.random()
            if roll > probability:
                logger.debug(
                    "sell overstock stop: itemid=%d stack=%d attempt=%d roll=%.4f required<=%.4f decay=%.4f",
                    itemid,
                    int(stack),
                    attempt,
                    roll,
                    probability,
                    self.sell_overstock_decay,
                )
                counts["overstock rate gate failed"] += 1
                break

            persona = self._choose_seller_persona()
            listing_price = self._sample_listing_price(base_price=price, stack=stack)
            self.seller.sell_item(
                itemid=itemid,
                stack=stack,
                date=self._sell_time,
                price=listing_price,
                count=1,
                seller=persona.seller,
                seller_name=persona.seller_name,
            )
            current_stock += 1
            counts["overstock sold"] += 1

        if counts:
            logger.debug("manager._sell_item counts itemid=%d stack=%d: %s", itemid, int(stack), dict(counts))

    def _sample_listing_price(self, base_price: int, stack: bool) -> int:
        """
        Sample a listing price around the configured base value.

        Args:
            base_price: Baseline listing price from the item config.
            stack: True when sampling a stack listing price.
        """
        del stack  # Reserved for future stack-specific behavior.

        jitter_min = self.sell_price_jitter_min_percent / 100.0
        jitter_max = self.sell_price_jitter_max_percent / 100.0
        magnitude = random.uniform(jitter_min, jitter_max)
        sign = 1 if random.random() < 0.6 else -1
        jitter_fraction = sign * magnitude
        sampled_price = int(round(base_price * (1 + jitter_fraction)))
        final_price = max(1, sampled_price)
        logger.debug(
            "sell price jitter sampled: base_price=%d stack=%d jitter_fraction=%.4f min_pct=%.2f max_pct=%.2f sampled_price=%d final_price=%d",
            base_price,
            int(stack),
            jitter_fraction,
            self.sell_price_jitter_min_percent,
            self.sell_price_jitter_max_percent,
            sampled_price,
            final_price,
        )
        return final_price
