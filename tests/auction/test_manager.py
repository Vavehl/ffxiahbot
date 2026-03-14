import pytest

from ffxiahbot.auction.manager import Manager, SellerPersona
from ffxiahbot.database import Database
from ffxiahbot.item import Item
from ffxiahbot.itemlist import ItemList
from ffxiahbot.tables.auctionhouse import AuctionHouse
from tests.cookbook import RANDOMIZE as R
from tests.cookbook import AuctionHouseRowBuilder as AHR
from tests.cookbook import setup_ah_transactions


@pytest.fixture()
def item_list() -> ItemList:
    # fmt: off
    return ItemList(
        items={
            1: Item(
                itemid=1, name="Fake Item A",
                sell_single=True, buy_single=True, price_single=100, stock_single=2, rate_single=1.0,
                sell_stacks=True, buy_stacks=True, price_stacks=200, stock_stacks=4, rate_stacks=1.0,
            ),
            2: Item(
                itemid=2, name="Fake Item B",
                sell_single=True, buy_single=True, price_single=200, stock_single=6, rate_single=1.0,
                sell_stacks=True, buy_stacks=True, price_stacks=400, stock_stacks=8, rate_stacks=1.0,
            ),
            4: Item(
                itemid=2, name="Fake Item C",
                sell_single=True, buy_single=True, price_single=600, stock_single=0, rate_single=1.0,
                sell_stacks=True, buy_stacks=True, price_stacks=800, stock_stacks=0, rate_stacks=1.0,
            ),
        }
    )
    # fmt: on


@pytest.mark.parametrize(
    "transactions,expected_blacklist,expect_updates",
    [
        pytest.param(
            [
                # 1: already bought (stack)
                AHR(price=100, seller=1, seller_name="A", itemid=1, stack=1, buyer_name="X", sell_date=R, sale=R),
                # 2: already bought (single)
                AHR(price=100, seller=1, seller_name="A", itemid=1, stack=0, buyer_name="X", sell_date=R, sale=R),
                # 3: item too expensive (stacks)
                AHR(price=300, seller=1, seller_name="A", itemid=1, stack=1),
                # 4: item too expensive (singles)
                AHR(price=300, seller=1, seller_name="A", itemid=1, stack=0),
                # 3: item not in database (stacks)
                AHR(price=100, seller=1, seller_name="A", itemid=3, stack=1),
                # 4: item not in database (singles)
                AHR(price=100, seller=1, seller_name="A", itemid=3, stack=0),
            ],
            {3, 4, 5, 6},
            {},
            id="Nothing to buy",
        ),
        pytest.param(
            [
                # 1: already bought (stack)
                AHR(price=100, seller=1, seller_name="A", itemid=1, stack=1, buyer_name="X", sell_date=R, sale=R),
                # 2: already bought (single)
                AHR(price=100, seller=1, seller_name="A", itemid=1, stack=0, buyer_name="X", sell_date=R, sale=R),
                # 3: item just right (stacks)
                AHR(price=200, seller=1, seller_name="A", itemid=1, stack=1),
                # 4: item just right (singles)
                AHR(price=100, seller=1, seller_name="A", itemid=1, stack=0),
                # 3: item not in database (stacks)
                AHR(price=100, seller=1, seller_name="A", itemid=3, stack=1),
                # 4: item not in database (singles)
                AHR(price=100, seller=1, seller_name="A", itemid=3, stack=0),
            ],
            {5, 6},
            {3, 4},
            id="Many things to buy",
        ),
    ],
)
def test_buy_items(
    populated_fake_db: Database,
    transactions: tuple[AHR, ...],
    expected_blacklist: set[int],
    expect_updates: set[int],
    item_list: ItemList,
) -> None:
    setup_ah_transactions(populated_fake_db, *transactions)
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
    )

    # perform buy loop multiple times
    for _ in range(3):
        manager.buy_items(item_list, use_buying_rates=False)
        assert manager.blacklist == expected_blacklist

    # validate database after buy loop
    for i in range(len(transactions)):
        ahr = transactions[i]
        with populated_fake_db.scoped_session() as session:
            row = session.query(AuctionHouse).filter(AuctionHouse.id == i + 1).one()
            if i + 1 in expect_updates:
                assert row.sell_date != 0
                assert row.sale == ahr.price
                assert row.buyer_name == "X"
            else:
                ahr.validate_query_result(row, id=i + 1)


def test_buy_items_records_executed_listing_price_not_max_cap(
    populated_fake_db: Database,
    item_list: ItemList,
) -> None:
    setup_ah_transactions(
        populated_fake_db,
        AHR(price=150, seller=1, seller_name="A", itemid=1, stack=1),
        AHR(price=80, seller=1, seller_name="A", itemid=1, stack=0),
    )

    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
    )

    manager.buy_items(item_list, use_buying_rates=False)

    with populated_fake_db.scoped_session() as session:
        stack_row = session.query(AuctionHouse).filter(AuctionHouse.id == 1).one()
        single_row = session.query(AuctionHouse).filter(AuctionHouse.id == 2).one()

    assert stack_row.sell_date != 0
    assert single_row.sell_date != 0
    assert stack_row.sale == 150
    assert single_row.sale == 80
    assert stack_row.buyer_name == "X"
    assert single_row.buyer_name == "X"


@pytest.mark.parametrize(
    "transactions,expected_history,expected_stock",
    [
        pytest.param(
            [
                *AHR.many(count=10, seller=1, seller_name="A", itemid=4, stack=1),
                *AHR.many(count=10, seller=1, seller_name="A", itemid=4, stack=0),
            ],
            {1: [100, 200], 2: [200, 400]},
            {1: [2, 4], 2: [6, 8], 3: [0, 0], 4: [10, 10]},
            id="Simple restock",
        ),
    ],
)
def test_restock_items(
    populated_fake_db: Database,
    transactions: tuple[AHR, ...],
    expected_history: dict[int, tuple[int, int]],
    expected_stock: dict[int, tuple[int, int]],
    item_list: ItemList,
) -> None:
    if transactions:
        setup_ah_transactions(populated_fake_db, *transactions)

    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
    )

    # perform restock loop multiple times
    for _ in range(3):
        manager.restock_items(item_list, use_selling_rates=False)

    # validate the historical prices after restock loop
    for itemid, (singles_price, stacks_price) in expected_history.items():
        with populated_fake_db.scoped_session() as session:
            stacks_row = (
                session.query(AuctionHouse)
                .filter(AuctionHouse.itemid == itemid, AuctionHouse.stack == 1, AuctionHouse.sale > 0)
                .one()
            )
            singles_row = (
                session.query(AuctionHouse)
                .filter(AuctionHouse.itemid == itemid, AuctionHouse.stack == 0, AuctionHouse.sale > 0)
                .one()
            )
            assert stacks_row.sale == stacks_price
            assert singles_row.sale == singles_price

    # validate the restocked items after restock loop
    for itemid, (singles_stock, stacks_stock) in expected_stock.items():
        with populated_fake_db.scoped_session() as session:
            stacks_rows = session.query(AuctionHouse).filter(
                AuctionHouse.itemid == itemid, AuctionHouse.stack == 1, AuctionHouse.sale == 0
            )
            singles_rows = session.query(AuctionHouse).filter(
                AuctionHouse.itemid == itemid, AuctionHouse.stack == 0, AuctionHouse.sale == 0
            )
            assert stacks_rows.count() == stacks_stock
            assert singles_rows.count() == singles_stock


def test_restock_items_uses_default_single_seller_when_pool_not_configured(
    populated_fake_db: Database,
    item_list: ItemList,
) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
    )

    manager.restock_items(item_list, use_selling_rates=False)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(AuctionHouse.sale == 0).all()
        assert rows
        assert {row.seller for row in rows} == {0}
        assert {row.seller_name for row in rows} == {"X"}


def test_restock_items_distributes_stock_across_seller_pool(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item_list = ItemList(
        items={
            1: Item(
                itemid=1,
                name="Fake Item A",
                sell_single=True,
                buy_single=True,
                price_single=100,
                stock_single=4,
                rate_single=1.0,
                sell_stacks=False,
                buy_stacks=False,
                price_stacks=0,
                stock_stacks=0,
                rate_stacks=1.0,
            ),
        }
    )

    pool = [
        SellerPersona(seller=11, seller_name="Alpha", weight=1.0),
        SellerPersona(seller=22, seller_name="Bravo", weight=1.0),
    ]
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        seller_pool=pool,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
    )

    picks = iter([pool[0], pool[1], pool[0], pool[1], pool[0]])
    monkeypatch.setattr("ffxiahbot.auction.manager.random.choices", lambda *_args, **_kwargs: [next(picks)])

    manager.restock_items(item_list, use_selling_rates=False)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        assert rows.count() == 4
        sellers = {row.seller for row in rows}
        assert sellers == {11, 22}
        seller_names = {(row.seller, row.seller_name) for row in rows}
        assert seller_names == {(11, "Alpha"), (22, "Bravo")}


def test_sample_listing_price_stays_within_bounds(populated_fake_db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=5.0,
        sell_price_jitter_max_percent=10.0,
    )

    values = iter([0.05, 0.10, 0.06, 0.07])
    signs = iter([0.2, 0.8, 0.2, 0.8])
    monkeypatch.setattr("ffxiahbot.auction.manager.random.uniform", lambda *_args, **_kwargs: next(values))
    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: next(signs))

    prices = [manager._sample_listing_price(base_price=100, stack=False) for _ in range(4)]

    assert prices == [105, 90, 106, 93]
    assert all(90 <= price <= 110 for price in prices)


def test_restock_items_uses_jittered_prices_for_active_listings(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item_list = ItemList(
        items={
            1: Item(
                itemid=1,
                name="Fake Item A",
                sell_single=True,
                buy_single=True,
                price_single=100,
                stock_single=3,
                rate_single=1.0,
                sell_stacks=False,
                buy_stacks=False,
                price_stacks=0,
                stock_stacks=0,
                rate_stacks=1.0,
            ),
        }
    )
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=5.0,
        sell_price_jitter_max_percent=10.0,
    )

    values = iter([0.05, 0.06, 0.07])
    signs = iter([0.2, 0.8, 0.2])
    monkeypatch.setattr("ffxiahbot.auction.manager.random.uniform", lambda *_args, **_kwargs: next(values))
    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: next(signs))

    manager.restock_items(item_list, use_selling_rates=False)

    with populated_fake_db.scoped_session() as session:
        history = (
            session.query(AuctionHouse)
            .filter(AuctionHouse.itemid == 1, AuctionHouse.stack == 0, AuctionHouse.sale > 0)
            .one()
        )
        listings = (
            session.query(AuctionHouse)
            .filter(AuctionHouse.itemid == 1, AuctionHouse.stack == 0, AuctionHouse.sale == 0)
            .order_by(AuctionHouse.id.asc())
            .all()
        )

    assert history.sale == 100
    listing_prices = [row.price for row in listings]
    assert listing_prices == [105, 94, 107]
    assert len(set(listing_prices)) > 1


def test_sell_item_overstock_disabled_has_no_extras(populated_fake_db: Database) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
        sell_overstock_attempt_cap=0,
    )

    manager._sell_item(itemid=1, stack=False, price=100, stock=2, rate=1.0)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        assert rows.count() == 2


def test_sell_item_overstock_first_extra_probability(populated_fake_db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
        sell_overstock_attempt_cap=3,
        sell_overstock_decay=0.5,
    )

    # stock fill (1) passes, first extra (0.49 <= 0.5) passes, second extra (0.3 > 0.25) fails.
    rolls = iter([0.0, 0.49, 0.3])
    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: next(rolls))

    manager._sell_item(itemid=1, stack=False, price=100, stock=1, rate=1.0)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        assert rows.count() == 2


def test_sell_item_overstock_subsequent_extras_are_rarer(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
        sell_overstock_attempt_cap=3,
        sell_overstock_decay=0.5,
    )

    # stock fill (0.0) pass, first extra threshold 0.5 and pass, second threshold 0.25 and fail.
    rolls = iter([0.0, 0.49, 0.26])
    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: next(rolls))

    manager._sell_item(itemid=1, stack=False, price=100, stock=1, rate=1.0)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        assert rows.count() == 2


def test_sell_item_overstock_extras_never_exceed_attempt_cap(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
        sell_overstock_attempt_cap=2,
        sell_overstock_decay=1.0,
    )

    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: 0.0)

    manager._sell_item(itemid=1, stack=False, price=100, stock=1, rate=1.0)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        # one guaranteed stock + two overstock attempts
        assert rows.count() == 3


@pytest.mark.parametrize(
    "current_stock,expected",
    [
        pytest.param(0, 1.0, id="low-pressure-boosted-and-clamped"),
        pytest.param(2, 0.8, id="at-target-uses-base-rate"),
        pytest.param(4, 0.0, id="high-pressure-suppressed"),
    ],
)
def test_effective_sell_rate_respects_pressure_and_bounds(current_stock: int, expected: float) -> None:
    rate = Manager._effective_sell_rate(rate=0.8, current_stock=current_stock, target_stock=2)
    assert rate == pytest.approx(expected)


def test_effective_sell_rate_monotonic_with_higher_pressure() -> None:
    low_pressure = Manager._effective_sell_rate(rate=0.6, current_stock=0, target_stock=2)
    target_pressure = Manager._effective_sell_rate(rate=0.6, current_stock=2, target_stock=2)
    high_pressure = Manager._effective_sell_rate(rate=0.6, current_stock=4, target_stock=2)

    assert low_pressure >= target_pressure >= high_pressure
    assert all(0.0 <= value <= 1.0 for value in (low_pressure, target_pressure, high_pressure))


def test_sell_item_high_pressure_blocks_restock_and_overstock(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
        sell_overstock_attempt_cap=2,
        sell_overstock_decay=1.0,
    )

    setup_ah_transactions(populated_fake_db, *AHR.many(count=4, seller=0, seller_name="X", itemid=1, stack=0))
    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: 0.0)

    manager._sell_item(itemid=1, stack=False, price=100, stock=2, rate=0.8)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        assert rows.count() == 4


def test_sell_item_low_pressure_allows_restock_and_overstock(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = Manager.from_db(
        populated_fake_db,
        name="X",
        rollback=True,
        fail=True,
        sell_price_jitter_min_percent=0.0,
        sell_price_jitter_max_percent=0.0,
        sell_overstock_attempt_cap=2,
        sell_overstock_decay=1.0,
    )

    monkeypatch.setattr("ffxiahbot.auction.manager.random.random", lambda: 0.0)

    manager._sell_item(itemid=1, stack=False, price=100, stock=2, rate=0.8)

    with populated_fake_db.scoped_session() as session:
        rows = session.query(AuctionHouse).filter(
            AuctionHouse.itemid == 1,
            AuctionHouse.stack == 0,
            AuctionHouse.sale == 0,
        )
        # 2 restock listings + 2 overstock listings
        assert rows.count() == 4


def test_manager_defaults_preserve_legacy_realism_behavior(populated_fake_db: Database) -> None:
    manager = Manager.from_db(populated_fake_db, name="X", rollback=True, fail=True)

    assert manager.sell_price_jitter_min_percent == 0.0
    assert manager.sell_price_jitter_max_percent == 0.0
    assert manager.sell_overstock_attempt_cap == 0
    assert manager.buy_price_slippage_percent == 0.0
    assert manager.use_seller_pool_weights is False


def test_choose_seller_persona_ignores_weights_when_disabled(
    populated_fake_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = [
        SellerPersona(seller=11, seller_name="Alpha", weight=10.0),
        SellerPersona(seller=22, seller_name="Bravo", weight=1.0),
    ]
    manager = Manager.from_db(populated_fake_db, name="X", seller_pool=pool, use_seller_pool_weights=False)

    captured: dict[str, object] = {}

    def fake_choices(_population, weights=None, k=1):
        captured["weights"] = weights
        return [pool[0]]

    monkeypatch.setattr("ffxiahbot.auction.manager.random.choices", fake_choices)

    manager._choose_seller_persona()
    assert captured["weights"] is None


def test_buy_row_slippage_zero_preserves_legacy_price_cap(populated_fake_db: Database) -> None:
    setup_ah_transactions(populated_fake_db, AHR(price=101, seller=1, seller_name="A", itemid=1, stack=0))
    manager = Manager.from_db(populated_fake_db, name="X", buy_price_slippage_percent=0.0)

    with populated_fake_db.scoped_session() as session:
        row = session.query(AuctionHouse).filter(AuctionHouse.id == 1).one()
        assert manager._buy_row(row, max_price=100) is False

    assert 1 in manager.blacklist
