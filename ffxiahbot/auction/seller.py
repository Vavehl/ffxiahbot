from dataclasses import dataclass, replace

from ffxiahbot.auction.worker import Worker
from ffxiahbot.logutils import capture
from ffxiahbot.tables.auctionhouse import AuctionHouse


@dataclass(frozen=True)
class Seller(Worker):
    """
    Auction House seller.

    Args:
        db: The database object.
        seller: The auction house seller id.
        seller_name: The auction house seller name.
    """

    #: The auction house seller id.
    seller: int
    #: The auction house seller name.
    seller_name: str

    def with_identity(self, seller: int | None = None, seller_name: str | None = None) -> Seller:
        """
        Create a copy with a different seller identity.

        Args:
            seller: Optional replacement seller id.
            seller_name: Optional replacement seller name.
        """
        return replace(
            self,
            seller=self.seller if seller is None else seller,
            seller_name=self.seller_name if seller_name is None else seller_name,
        )

    def set_history(
        self,
        itemid: int,
        stack: int | bool,
        price: int,
        date: int,
        count: int = 1,
        seller: int | None = None,
        seller_name: str | None = None,
    ) -> None:
        """
        Set the history of a particular item.

        Args:
            itemid: The item number.
            stack: The stack size.
            price: The price.
            date: The timestamp.
            count: The number of rows.
            seller: Optional seller id override.
            seller_name: Optional seller name override.
        """
        with capture(fail=self.fail):
            itemid = AuctionHouse.validate_itemid(itemid)
            stack = AuctionHouse.validate_stack(stack)
            price = AuctionHouse.validate_price(price)
            date = AuctionHouse.validate_date(date)
            seller = AuctionHouse.validate_seller(self.seller if seller is None else seller)
            seller_name = self.seller_name if seller_name is None else seller_name

            # add row
            with self.scoped_session() as session:
                # add the item multiple times
                for _i in range(count):
                    row = AuctionHouse(
                        itemid=itemid,
                        stack=stack,
                        seller=seller,
                        seller_name=seller_name,
                        date=date,
                        price=price,
                        buyer_name=seller_name,
                        sale=price,
                        sell_date=date,
                    )
                    session.add(row)

    def sell_item(
        self,
        itemid: int,
        stack: int,
        date: int,
        price: int,
        count: int,
        seller: int | None = None,
        seller_name: str | None = None,
    ) -> None:
        """
        Put up a particular item for sale.

        Args:
            itemid: The item number.
            stack: The stack size.
            date: The timestamp.
            price: The price.
            count: The number of rows.
            seller: Optional seller id override.
            seller_name: Optional seller name override.
        """
        with capture(fail=self.fail):
            itemid = AuctionHouse.validate_itemid(itemid)
            stack = AuctionHouse.validate_stack(stack)
            price = AuctionHouse.validate_price(price)
            date = AuctionHouse.validate_date(date)
            seller = AuctionHouse.validate_seller(self.seller if seller is None else seller)
            seller_name = self.seller_name if seller_name is None else seller_name

            # add row
            with self.scoped_session() as session:
                # add the item multiple times
                for _i in range(count):
                    row = AuctionHouse(
                        itemid=itemid,
                        stack=stack,
                        seller=seller,
                        seller_name=seller_name,
                        date=date,
                        price=price,
                    )
                    session.add(row)
