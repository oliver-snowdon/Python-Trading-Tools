import mysql.connector
import Config

class Database:

	def __init__(self):
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
			cursor.execute('SELECT `id` FROM `pairs` WHERE `pair` = "{}"'.format(pair))
			for row in cursor:
				pairIds[pair] = row[0]
			if pair not in pairIds:
				cursor.execute('INSERT INTO `pairs` (`pair`) VALUES ("{}");'.format(pair))
				self.cnx.commit()
				pairIds[pair] = cursor.lastrowid
		return pairIds

	def AddSpreadUpdate(self, pairId, ask, bid, timestamp):
		cursor = self.cnx.cursor()
		cursor.execute('INSERT INTO `spreads` (`pair_id`, `ask`, `bid`, `timestamp`) VALUES ({}, {}, {}, {});'
			       .format(pairId, ask, bid, timestamp))
		self.cnx.commit()

	def AddTrade(self, pairId, price, amount, timestamp, buyOrSell, marketOrLimit, misc):
		cursor = self.cnx.cursor()
		cursor.execute('INSERT INTO `trades` (`pair_id`, `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc`) VALUES ({}, {}, {}, {}, "{}", "{}", "{}");'
			       .format(pairId, price, amount, timestamp, buyOrSell, marketOrLimit, misc))
		self.cnx.commit()
