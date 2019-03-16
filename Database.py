import mysql.connector
import Config
import sys

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
		cursor.execute("UPDATE `runs` SET `error` = %s WHERE id = %s;", (error, self.runId))
		self.cnx.commit()
		self.StartRun()
	
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

	def GetNonEmptyLocalRuns(self):
		self.cnx.commit()
		cursor = self.cnx.cursor(buffered=True)
		cursor.execute('SELECT `id` FROM `runs` WHERE `node` = "localhost";')
		result = []
		for row in cursor:
			runId = row[0]
			runStart, runEnd = self.GetRunRange(runId)
			if runStart != None and runEnd != None:
				result.append({"id":runId, "start":str(runStart), "end":str(runEnd)})
			print(runId)
			sys.stdout.flush()
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
