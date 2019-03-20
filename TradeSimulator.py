class TradeSimulator:

	def __init__(self, initialBaseBalance, initialQuoteBalance):
		self.baseBalance = initialBaseBalance
		self.quoteBalance = initialQuoteBalance

	def Buy(self, volume, ask, exchangeFee):
		self.baseBalance = self.baseBalance + volume
		self.quoteBalance = self.quoteBalance - (1 + exchangeFee) * volume * ask

	def Sell(self, volume, bid, exchangeFee):
		self.baseBalance = self.baseBalance - volume
		self.quoteBalance = self.quoteBalance + (1 - exchangeFee) * volume * bid

	def GetEquity(self, bid):
		return self.quoteBalance + bid * self.baseBalance

	def GetBaseBalance(self):
		return self.baseBalance

	def GetQuoteBalance(self):
		return self.quoteBalance
