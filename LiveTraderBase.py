from GetPrices import GetPrices
from TimerThread import TimerThread
from Database import Database
from Kraken.ExchangeHandle import ExchangeHandle
import time
from TradeSimulator import TradeSimulator
from GapFiller import FillGap, DownloadRuns
import sys

class LiveTraderBase(TimerThread):

	def __init__(self, pairWsname, baseName, quoteName, simulation=True):
		super(LiveTraderBase, self).__init__(1)
		self.pairWsname = pairWsname
		self.baseName = baseName
		self.quoteName = quoteName
		self.database = Database()
		self.secondsUntilNextTradeDecision = 0
		self.syncPeriod = 60
		self.secondsUntilSync = self.syncPeriod
		self.minimumOrder = 0.02
		self.simulation=simulation
		if simulation:
			self.simulator = TradeSimulator('SimulatorBalances.json')
		else:
			self.exchangeHandle = ExchangeHandle('Kraken/kraken.key')

	def Target(self):
		super(LiveTraderBase, self).Target()
		self.secondsUntilSync = self.secondsUntilSync - 1
		if self.secondsUntilSync <= 0 and not self.simulation:
			self.exchangeHandle.SyncBalances()
			self.exchangeHandle.SyncOpenPositions()
			self.secondsUntilSync = self.syncPeriod
		self.secondsUntilNextTradeDecision = self.secondsUntilNextTradeDecision - 1
		if self.secondsUntilNextTradeDecision <= 0:
			lookback = self.SecondsLookbackRequiredForNextDecision()
			dataStartTimestamp = int(time.time()) - lookback
			remoteRuns = DownloadRuns(self.pairWsname)
			FillGap(self.database, self.pairWsname, dataStartTimestamp, dataStartTimestamp+lookback, remoteRuns)
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
				if difference > self.minimumOrder:
					self.PlaceMarketOrder(abs(difference), 'buy', asks[-1], bids[-1])
				elif difference < -self.minimumOrder:
					self.PlaceMarketOrder(abs(difference), 'sell', asks[-1], bids[-1])
				else:
					print("Difference between target amount and base balance less than minimum order")

				print("Base balance = {}".format(self.GetBaseBalance()))
				print("Quote balance = {}".format(self.GetQuoteBalance()))
		sys.stdout.flush()

	def PlaceMarketOrder(self, volume, buyOrSell, ask, bid):
		if self.simulation:
			if buyOrSell == 'buy':
				self.simulator.Buy(volume, ask, 0.0026)
			elif buyOrSell == 'sell':
				self.simulator.Sell(volume, bid, 0.0026)
		else:
			if buyOrSell == 'buy':
				self.exchangeHandle.PlaceMarketOrder(difference, pair, 'buy', leverage='5:1')
			elif buyOrSell == 'sell':
				self.exchangeHandle.PlaceMarketOrder(difference, pair, 'sell', leverage='5:1')

	def GetBaseBalance(self):
		if self.simulation:
			result =  self.simulator.GetBaseBalance()
		else:
			result = self.exchangeHandle.GetBalance(self.baseName)\
			       + self.exchangeHandle.GetOpenPositionBalance(self.baseName)
		return result

	def GetQuoteBalance(self):
		if self.simulation:
			result = self.simulator.GetQuoteBalance()
		else:
			result = self.exchangeHandle.GetBalance(self.quoteName)
		return result

	def GetEquity(self, bid):
		if self.simulation:
			return self.simulator.GetQuoteBalance()\
			     + self.simulator.GetBaseBalance() * bid
		else:
			return self.exchangeHandle.GetEquity(self.quoteName)

	def SecondsLookbackRequiredForNextDecision(self):
		return 60*10*24

	def MakeTradeDecision(self, asks, bids):
		return 10, 0.5

if __name__ == '__main__':
	trader = LiveTraderBase('ETH/EUR', 'XETH', 'ZEUR')
	print(trader.GetBaseBalance())
	print(trader.GetQuoteBalance())
	trader.Start()
