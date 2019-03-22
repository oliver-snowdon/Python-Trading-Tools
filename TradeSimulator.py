import json

class TradeSimulator:

	def __init__(self, simulatorBalancesFilename):
		self.simulatorBalancesFilename = simulatorBalancesFilename
		with open(simulatorBalancesFilename) as jsonFile:
			balances = json.load(jsonFile)
		self.baseBalance = balances['base']
		self.quoteBalance = balances['quote']

	def Buy(self, volume, ask, exchangeFee):
		self.baseBalance = self.baseBalance + volume
		self.quoteBalance = self.quoteBalance - (1 + exchangeFee) * volume * ask
		self.SaveState()

	def Sell(self, volume, bid, exchangeFee):
		self.baseBalance = self.baseBalance - volume
		self.quoteBalance = self.quoteBalance + (1 - exchangeFee) * volume * bid
		self.SaveState()

	def GetEquity(self, bid):
		return self.quoteBalance + bid * self.baseBalance

	def GetBaseBalance(self):
		return self.baseBalance

	def GetQuoteBalance(self):
		return self.quoteBalance

	def SaveState(self):
		balances = {'base':self.baseBalance, 'quote':self.quoteBalance}
		with open(self.simulatorBalancesFilename, 'w') as jsonFile:
			json.dump(balances, jsonFile)
