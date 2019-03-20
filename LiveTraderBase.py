from GetPrices import GetPrices
from TimerThread import TimerThread
from Database import Database
from Kraken.ExchangeHandle import ExchangeHandle
import time

class LiveTraderBase(TimerThread):

	def __init__(self, pairWsname, baseName, quoteName):
		super(LiveTraderBase, self).__init__(1)
		self.pairWsname = pairWsname
		self.baseName = baseName
		self.quoteName = quoteName
		self.database = Database()
		self.secondsUntilNextTradeDecision = 0
		self.syncPeriod = 60
		self.secondsUntilSync = self.syncPeriod
		self.exchangeHandle = ExchangeHandle('Kraken/kraken.key')
		self.minimumOrder = 0.02

	def Target(self):
		super(LiveTraderBase, self).Target()
		self.secondsUntilSync = self.secondsUntilSync - 1
		if self.secondsUntilSync <= 0:
			self.exchangeHandle.SyncBalances()
			self.exchangeHandle.SyncOpenPositions()
			print("Equity is {}".format(self.exchangeHandle.GetEquity()))
			self.secondsUntilSync = self.syncPeriod
		self.secondsUntilNextTradeDecision = self.secondsUntilNextTradeDecision - 1
		if self.secondsUntilNextTradeDecision <= 0:
			lookback = self.SecondsLookbackRequiredForNextDecision()
			dataStartTimestamp = int(time.time()) - lookback
			print("dataStartTimestamp: {}".format(dataStartTimestamp))
			print("lookback: {}".format(lookback))
			while True:
				try:
					asks, bids = GetPrices(self.database, self.pairWsname, dataStartTimestamp, 1, lookback)
					break
				except Exception as error:
					print("Caught this error: {}".format(repr(error)))
					time.sleep(1)
			self.secondsUntilNextTradeDecision, targetAmount = self.MakeTradeDecision(asks, bids)
			if targetAmount != self.GetBaseBalance():
				difference = targetAmount - self.GetBaseBalance()
				pair = '{}{}'.format(self.baseName, self.quoteName)
				if difference > 0:
					self.exchangeHandle.PlaceMarketOrder(difference, pair, 'buy', leverage='5:1')
				elif difference < 0:
					self.exchangeHandle.PlaceMarketOrder(difference, pair, 'sell', leverage='5:1')
				else:
					print("Difference between target amount and base balance less than minimum order")

	def GetBaseBalance(self):
		return self.exchangeHandle.GetBalance(self.baseName) + self.exchangeHandle.GetOpenPositionBalance(self.baseName)

	def GetQuoteBalance(self):
		return self.exchangeHandle.GetBalance(self.quoteName)

	def GetEquity(self):
		return self.exchangeHandle.GetEquity(self.quoteName)

	def SecondsLookbackRequiredForNextDecision(self):
		return 60*10*24

	def MakeTradeDecision(self, asks, bids):
		return 10, 0

if __name__ == '__main__':
	trader = LiveTraderBase('ETH/EUR', 'XETH', 'ZEUR')
	print(trader.GetBaseBalance())
	print(trader.GetQuoteBalance())
	print(trader.GetEquity())
	trader.Start()
