import mysql.connector
import Config
import sys
import json

class Database:

	def __init__(self):
		self.Connect()
		self.runId = -1

	def Connect(self):
		self.cnx = mysql.connector.connect(user=Config.user,
						   password=Config.password,
						   host=Config.host,
						   database=Config.database)

	def SetupDatabase(self):
		cursor = self.cnx.cursor()
		cursor.execute("SHOW TABLES;")
		cursor.fetchall()
		if cursor.rowcount > 0:
			print('Database already has tables. This probably means you have already called SetupDatabase.')
			quit()

		sql = open("Database.sql", 'r').read()
		cursor.execute(sql);

	def GetPairIds(self, pairs):
		cursor = self.cnx.cursor()
		pairIds = dict()
		for pair in pairs:
			cursor.execute("SELECT `id` FROM `pairs` WHERE `pair` = %s;", (pair,))
			for row in cursor:
				pairIds[pair] = row[0]
			if pair not in pairIds:
				cursor.execute("INSERT INTO `pairs` (`pair`) VALUES (%s);", (pair,))
				self.cnx.commit()
				pairIds[pair] = cursor.lastrowid
		return pairIds

	def GetPairsForRun(self, runId):
		cursor = self.cnx.cursor()
		cursor.execute("SELECT `pairs` FROM `runs` WHERE `id` = %s;", (runId,))
		for row in cursor:
			return json.loads(row[0])
		return None

	def CalculatePairsForRun(self, runId):
		cursor = self.cnx.cursor()
		pairIds = set()
		cursor.execute("SELECT `pair_id` FROM `spreads` WHERE `run_id` = %s;", (runId,))
		for row in cursor:
			pairIds.add(row[0])
		cursor.execute("SELECT `pair_id` FROM `trades` WHERE `run_id` = %s;", (runId,))
		for row in cursor:
			pairIds.add(row[0])
		result = set()
		for pairId in pairIds:
			cursor.execute("SELECT `pair` FROM `pairs` WHERE `id` = %s;", (pairId,))
			result.add(cursor.fetchone()[0])
		return result
		
	def StartRun(self, pairs):
		cursor = self.cnx.cursor()
		cursor.execute('INSERT INTO `runs` (`node`, `error`, `pairs`) VALUES ("localhost", "", %s);', (json.dumps(pairs),))
		self.cnx.commit()
		self.runId = cursor.lastrowid
		
	def LogError(self, error, pairs):
		self.Connect()
		cursor = self.cnx.cursor()
		cursor.execute("UPDATE `runs` SET `error` = %s WHERE id = %s;", (error, self.runId))
		self.cnx.commit()
		self.StartRun(pairs)
	
	def AddSpreadUpdate(self, pairId, ask, bid, timestamp):
		cursor = self.cnx.cursor()
		cursor.execute('INSERT INTO `spreads` (`pair_id`, `run_id`, `ask`, `bid`, `timestamp`) VALUES ({}, {}, {}, {}, {});'
			       .format(pairId, self.runId, ask, bid, timestamp))
		self.cnx.commit()
		self.UpdateRunStats(self.runId, timestamp)

	def AddTrade(self, pairId, price, amount, timestamp, buyOrSell, marketOrLimit, misc):
		cursor = self.cnx.cursor()
		cursor.execute("INSERT INTO `trades` (`pair_id`, `run_id`, `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc`) VALUES ({}, {}, {}, {}, {}, %s, %s, %s);"
			       .format(pairId, self.runId, price, amount, timestamp), (buyOrSell, marketOrLimit, misc))
		self.cnx.commit()
		self.UpdateRunStats(self.runId, timestamp)

	def GetRunRange(self, runId):
		cursor = self.cnx.cursor()
		cursor.execute("SELECT `first_timestamp`, `last_timestamp` FROM `runs` WHERE `id` = {}".format(runId))
		firstTimestamp, lastTimestamp = cursor.fetchone()
		return firstTimestamp, lastTimestamp

	def CalculateRunRange(self, runId, cursor=None):
		if cursor == None:
			self.cnx.commit()
			cursor = self.cnx.cursor()
		cursor.execute("SELECT COUNT(1) FROM `spreads` WHERE `run_id` = %s", (runId,))
		nSpreads = cursor.fetchone()[0]
		minSpreadTimestamp = None
		maxSpreadTimestamp = None
		if nSpreads > 0:
			cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM `spreads` WHERE `run_id` = %s;", (runId,))
			row = cursor.fetchone()
			minSpreadTimestamp = row[0]
			maxSpreadTimestamp = row[1]
		cursor.execute("SELECT COUNT(1) FROM `trades` WHERE `run_id` = %s", (runId,))
		nTrades = cursor.fetchone()[0]
		minTradeTimestamp = None
		maxTradeTimestamp = None
		if nSpreads > 0:
			cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM `trades` WHERE `run_id` = %s;", (runId,))
			row = cursor.fetchone()
			minTradeTimestamp = row[0]
			maxTradeTimestamp = row[1]
		if minSpreadTimestamp != None and minTradeTimestamp != None:
			return min(minSpreadTimestamp, minTradeTimestamp), max(maxSpreadTimestamp, maxTradeTimestamp)
		elif minSpreadTimestamp:
			return minSpreadTimestamp, maxSpreadTimestamp
		elif minTradeTimestamp:
			return minTradeTimestamp, maxTradeTimestamp
		else:
			return 0, 0

	def SyncRunStats(self):
		self.cnx.commit()
		cursor = self.cnx.cursor(buffered=True)
		cursor.execute('SELECT `id`, `first_timestamp`, `last_timestamp` FROM `runs` WHERE `node` = "localhost";')
		for row in cursor:
			if row[1] == None and row[2] == None:
				start, end = self.CalculateRunRange(row[0])
				updateCursor = self.cnx.cursor()
				updateCursor.execute("UPDATE `runs` SET `first_timestamp` = {}, `last_timestamp` = {} WHERE `id` = {}"
						     .format(start, end, row[0]))
		self.cnx.commit()

	def SyncRunPairs(self):
		self.cnx.commit()
		cursor = self.cnx.cursor(buffered=True)
		cursor.execute("SELECT `id` FROM `runs` WHERE `pairs` = \"\";")
		for row in cursor:
			runId = row[0]
			print(runId)
			updateCursor = self.cnx.cursor()
			updateCursor.execute("UPDATE `runs` SET `pairs` = %s WHERE `id` = %s",
					     (json.dumps(list(self.CalculatePairsForRun(runId))), runId))
		self.cnx.commit()

	def UpdateRunStats(self, runId, activityTimestamp):
		cursor = self.cnx.cursor()
		cursor.execute("SELECT `first_timestamp`, `last_timestamp` FROM `runs` WHERE `id` = {}".format(runId))
		firstTimestamp, lastTimestamp = cursor.fetchone()
		if firstTimestamp == None:
			cursor.execute("UPDATE `runs` SET `first_timestamp` = {}, `last_timestamp` = {} WHERE `id` = {}"
				       .format(activityTimestamp, activityTimestamp, runId))
		else:
			cursor.execute("UPDATE `runs` SET `last_timestamp` = {} WHERE `id` = {}"
				       .format(activityTimestamp, runId))
		self.cnx.commit()

	def GetLocalOverlappingRun(self, interruptionStart, interruptionEnd):
		self.cnx.commit()
		cursor = self.cnx.cursor(buffered=True)
		cursor.execute('SELECT `id` FROM `runs` WHERE `node` = "localhost";')
		result = -1
		for row in cursor:
			runId = row[0]
			runStart, runEnd = self.GetRunRange(runId)
			if result < 0 and runStart < interruptionStart and runEnd > interruptionEnd:
				result = runId
		return result

	def GetNonEmptyRuns(self):
		self.cnx.commit()
		cursor = self.cnx.cursor(buffered=True)
		cursor.execute('SELECT `id`, `pairs`, `first_timestamp`, `last_timestamp` FROM `runs`;')
		result = []
		for row in cursor:
			runId = row[0]
			runStart = row[2]
			runEnd = row[3]
			if runStart != None and runEnd != None and row[1] != "[]":
				result.append({"id":runId, "start":str(runStart), "end":str(runEnd), "pairs": json.loads(row[1])})
#			print(runId)
#			sys.stdout.flush()
		return result

	def CountSpreads(self, pairId, startTimestamp, timestampUpTo):
		self.cnx.commit()
		cursor = self.cnx.cursor()
		cursor.execute('SELECT COUNT(1) FROM `spreads` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {};'
			       .format(startTimestamp, timestampUpTo, pairId))
		result = cursor.fetchone()[0]
		return result

	def CountTrades(self, pairId, startTimestamp, timestampUpTo):
		self.cnx.commit()
		cursor = self.cnx.cursor()
		cursor.execute('SELECT COUNT(1) FROM `trades` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {};'
			       .format(startTimestamp, timestampUpTo, pairId))
		result = cursor.fetchone()[0]
		return result

	def GetSpreads(self, runId, pairId, startTimestamp, timestampUpTo):
		self.cnx.commit()
		cursor = self.cnx.cursor()
		cursor.execute('SELECT `ask`, `bid`, `timestamp` FROM `spreads` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {} AND `run_id` = {} ORDER BY `timestamp` ASC;'
			       .format(startTimestamp, timestampUpTo, pairId, runId))
		result = []
		for row in cursor:
			result.append({'ask': str(row[0]), 'bid': str(row[1]), 'timestamp': str(row[2])})
		return result

	def GetTrades(self, runId, pairId, startTimestamp, timestampUpTo):
		self.cnx.commit()
		cursor = self.cnx.cursor()
		cursor.execute('SELECT `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc` FROM `trades` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {} AND `run_id` = {} ORDER BY `timestamp` ASC;'
			       .format(startTimestamp, timestampUpTo, pairId, runId))
		result = []
		for row in cursor:
			result.append({'price': str(row[0]), 'amount': str(row[1]), 'timestamp': str(row[2]),
				       'buy_or_sell': row[3], 'market_or_limit': row[4], 'misc': row[5]})
		return result
