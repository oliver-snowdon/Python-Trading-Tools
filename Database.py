import mysql.connector
import Config

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
		
	def StartRun(self):
		cursor = self.cnx.cursor()
		cursor.execute('INSERT INTO `runs` (`node`, `error`) VALUES ("localhost", "");')
		self.cnx.commit()
		self.runId = cursor.lastrowid
		
	def LogError(self, error):
		self.Connect()
		cursor = self.cnx.cursor()
		cursor.execute("UPDATE `runs` SET `error` = %s WHERE id = %d;", (error, self.runId))
		self.cnx.commit()
		self.StartRun()
	
	def AddSpreadUpdate(self, pairId, ask, bid, timestamp):
		cursor = self.cnx.cursor()
		cursor.execute('INSERT INTO `spreads` (`pair_id`, `run_id`, `ask`, `bid`, `timestamp`) VALUES ({}, {}, {}, {}, {});'
			       .format(pairId, self.runId, ask, bid, timestamp))
		self.cnx.commit()

	def AddTrade(self, pairId, price, amount, timestamp, buyOrSell, marketOrLimit, misc):
		cursor = self.cnx.cursor()
		cursor.execute("INSERT INTO `trades` (`pair_id`, `run_id`, `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc`) VALUES ({}, {}, {}, {}, {}, %s, %s, %s);"
			       .format(pairId, self.runId, price, amount, timestamp), (buyOrSell, marketOrLimit, misc))
		self.cnx.commit()

	def GetRunRange(self, runId):
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

	def GetLocalOverlappingRun(self, interruptionStart, interruptionEnd):
		cursor = self.cnx.cursor(buffered=True)
		cursor.execute('SELECT `id` FROM `runs` WHERE `node` = "localhost";')
		result = -1
		for row in cursor:
			runId = row[0]
			runStart, runEnd = self.GetRunRange(runId)
			if result < 0 and runStart < interruptionStart and runEnd > interruptionEnd:
				result = runId
		return result

	def CountSpreads(self, pairId, startTimestamp, timestampUpTo):
		cursor = self.cnx.cursor()
		cursor.execute('SELECT COUNT(1) FROM `spreads` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {};'
			       .format(startTimestamp, timestampUpTo, pairId))
		result = cursor.fetchone()[0]
		self.cnx.commit()
		return result

	def CountTrades(self, pairId, startTimestamp, timestampUpTo):
		cursor = self.cnx.cursor()
		cursor.execute('SELECT COUNT(1) FROM `trades` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {};'
			       .format(startTimestamp, timestampUpTo, pairId))
		result = cursor.fetchone()[0]
		self.cnx.commit()
		return result

	def GetSpreads(self, pairId, startTimestamp, timestampUpTo):
		cursor = self.cnx.cursor()
		cursor.execute('SELECT `ask`, `bid`, `timestamp` FROM `spreads` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {};'
			       .format(startTimestamp, timestampUpTo, pairId))
		result = []
		for row in cursor:
			result.append({'ask': str(row[0]), 'bid': str(row[1]), 'timestamp': str(row[2])})
		self.cnx.commit()
		return result

	def GetTrades(self, pairId, startTimestamp, timestampUpTo):
		cursor = self.cnx.cursor()
		cursor.execute('SELECT `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc` FROM `trades` WHERE timestamp >= {} AND timestamp < {} AND `pair_id` = {};'
			       .format(startTimestamp, timestampUpTo, pairId))
		result = []
		for row in cursor:
			result.append({'price': str(row[0]), 'amount': str(row[1]), 'timestamp': str(row[2]),
				       'buy_or_sell': row[3], 'market_or_limit': row[4], 'misc': row[5]})
		self.cnx.commit()
		return result
