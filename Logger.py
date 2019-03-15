import asyncio
import sys
from Kraken import WebsocketLogger, RESTInterface
from Database import Database
import time

pairs = RESTInterface.GetPairs()

while True:
	try:
		database = Database()
		pairIds = database.GetPairIds(pairs)
		print(pairIds)
		break
	except Exception as error:
		print('Caught this error while opening database: ' + repr(error))
		sys.stdout.flush()
		time.sleep(3)

asyncio.get_event_loop().run_until_complete(WebsocketLogger.Stream(pairs, database, pairIds))
