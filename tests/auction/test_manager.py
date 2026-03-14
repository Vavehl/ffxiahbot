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
    manager = Manager.from_db(populated_fake_db, name="X", rollback=True, fail=True)

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

    manager = Manager.from_db(populated_fake_db, name="X", rollback=True, fail=True)

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
    manager = Manager.from_db(populated_fake_db, name="X", rollback=True, fail=True)

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
    manager = Manager.from_db(populated_fake_db, name="X", rollback=True, fail=True, seller_pool=pool)

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
