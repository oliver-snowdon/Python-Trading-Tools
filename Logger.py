import asyncio
from Kraken import WebsocketLogger
from Database import Database

pairs = ["XBT/EUR", "ETH/EUR"]

database = Database()
pairIds = database.GetPairIds(pairs)
print(pairIds)
asyncio.get_event_loop().run_until_complete(WebsocketLogger.Stream(pairs, database, pairIds))
