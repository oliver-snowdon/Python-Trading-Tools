import asyncio
from Kraken import WebsocketLogger
from Database import Database

pairs = ["XBT/EUR", "ETH/EUR"]

while True:
	try:
		database = Database()
		break
	except Exception as error:
		print('Caught this error while opening database: ' + repr(error))
		time.sleep(3)

pairIds = database.GetPairIds(pairs)
print(pairIds)
asyncio.get_event_loop().run_until_complete(WebsocketLogger.Stream(pairs, database, pairIds))
