import json, time
import asyncio
import websockets
import sys
from decimal import Decimal

async def Stream(pairs, database, pairIds):
	while True:
		try:
			database.StartRun()
			break
		except Exception as error:
			print('Caught this error while starting: ' + repr(error))
			sys.stdout.flush()
			time.sleep(3)

	while True:
		try:
			async with websockets.connect('wss://ws.kraken.com') as websocket:

				await websocket.send(json.dumps({"event": "subscribe",
								 "pair": pairs,
								 "subscription": {"name": "spread"}}))

				await websocket.send(json.dumps({"event": "subscribe",
								 "pair": pairs,
								 "subscription": {"name": "trade"}}))


				channelDict = dict()

				while True:
					try:
						data = await websocket.recv()
						obj = json.loads(data)
						if isinstance(obj, dict):
							if "error" in obj and obj["error"] != "":
								raise Exception("error: {}".format(obj["error"]))
							if "event" in obj and obj["event"] == "subscriptionStatus":
								subscriptionName = obj["subscription"]["name"]
								channelDict[obj["channelID"]] = {"pair":obj["pair"],
												 "name":subscriptionName}
						elif isinstance(obj, list):
							channelId = obj[0]
							channel = channelDict[channelId]
							pair = channel["pair"]
							if channel["name"] == "spread":
								bid = Decimal(obj[1][0])
								ask = Decimal(obj[1][1])
								timestamp = Decimal(obj[1][2])
								#print("Pair: {}; Ask: {}; Bid: {}; Timestamp: {}"
								#      .format(pair, ask, bid, timestamp))
								database.AddSpreadUpdate(pairIds[pair], ask, bid, timestamp)
							elif channel["name"] == "trade":
								price = Decimal(obj[1][0][0])
								amount = Decimal(obj[1][0][1])
								timestamp = Decimal(obj[1][0][2])
								buyOrSell = obj[1][0][3]
								marketOrLimit = obj[1][0][4]
								misc = obj[1][0][5]
								#print("Pair: {}; Price: {}; Amount: {}; Timestamp: {}; {}; {}; {};"
								#      .format(pair, price, amount, timestamp,
								#	      buyOrSell, marketOrLimit, misc))
								database.AddTrade(pairIds[pair], price, amount, timestamp,
										  buyOrSell, marketOrLimit, misc)

					except Exception as error:
						print('Caught this error: ' + repr(error))
						sys.stdout.flush()
						print('Data was '.format(data))
						sys.stdout.flush()
						while True:
							try:
								database.LogError(repr(error))
								break
							except Exception as innerError:
								print('Caught this error while handling original error: ' + repr(innerError))
								sys.stdout.flush()
								time.sleep(3)
						time.sleep(3)
						break
		except Exception as error:
			print('Caught this error: ' + repr(error))
			sys.stdout.flush()
			while True:
				try:
					database.LogError(repr(error))
					break
				except Exception as innerError:
					print('Caught this error while handling original error: ' + repr(innerError))
					sys.stdout.flush()
					time.sleep(3)
			time.sleep(3)
