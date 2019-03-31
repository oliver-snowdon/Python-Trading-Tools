from decimal import Decimal
import numpy as np
from Database import Database
from Kraken import RESTInterface
import time

def GetPrices(database, pair, startTimestamp, interval, timeSteps, verbose=False):
	pairIds = database.GetPairIds([pair])
	pairId = pairIds[pair]

	database.cnx.commit()
	preQueryCursor = database.cnx.cursor()
	preQueryCursor.execute('SELECT MIN(first_timestamp) FROM `runs` WHERE `pairs` LIKE "%{}%"'.format(pair))
	minTimestampInDatabase = preQueryCursor.fetchone()[0]
	preQueryCursor.execute('SELECT MAX(last_timestamp) FROM `runs` WHERE `pairs` LIKE "%{}%"'.format(pair))
	maxTimestampInDatabase = preQueryCursor.fetchone()[0]

	if minTimestampInDatabase > startTimestamp:
		raise Exception("Not enough data at beginning")
	if maxTimestampInDatabase < startTimestamp + interval*timeSteps:
		raise Exception("Not enough data at end")

	asks = np.zeros(timeSteps)
	bids = np.zeros(timeSteps)

	cursor = database.cnx.cursor()
	cursor.execute('SELECT `ask`, `bid`, `timestamp` FROM `spreads` WHERE `pair_id` = %s AND `timestamp` >= %s AND `timestamp` <= %s ORDER BY `timestamp` ASC;',
		       (pairId, startTimestamp, startTimestamp + interval*(timeSteps-1)))
	i = 0
	for row in cursor:
		ask = Decimal(row[0])
		bid = Decimal(row[1])
		timestamp = Decimal(row[2])
		asks[i] = ask
		bids[i] = bid
		while True:
			if timestamp > startTimestamp + interval*i:
				i = i+1
				if verbose:
					print("{} of {}.".format(i, timeSteps))
				asks[i] = ask
				bids[i] = bid
			else:
				break
	for j in range(i, timeSteps):
		asks[j] = ask
		bids[j] = bid

	return asks, bids

if __name__ == "__main__":
	database = Database()
	timeSteps = 60*60*24*15
	start = 1552694400
	#pairs = RESTInterface.GetPairs()
	pairs = ["ETH/EUR"]
	for pair in pairs:
		asks, bids = GetPrices(database, pair, start, 1, timeSteps, verbose=True)
		np.save("Data/{}_asks.npy".format(pair.replace('/', '.')), asks)
		np.save("Data/{}_bids.npy".format(pair.replace('/', '.')), bids)
		print(pair)
