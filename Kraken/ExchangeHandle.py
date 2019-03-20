import sys
import os
import os.path
import time

thisFile = os.path.realpath(__file__)
directory = os.path.dirname(thisFile)

sys.path.append(os.path.join(directory, 'python3-krakenex'))

import krakenex
from decimal import Decimal

class ExchangeHandle:
	
	def __init__(self, keyPath):
		self.k = krakenex.API()
		self.k.load_key(keyPath)
		self.SyncOpenPositions()
		self.SyncBalances()
		self.InitPairs()

	def SyncBalances(self):
		self.balances = self.TryQueryPrivateUntilSuccess('Balance')
		print(self.balances)

	def SyncOpenPositions(self):
		result = self.TryQueryPrivateUntilSuccess('OpenPositions', data={'docalcs': 'true'})

		self.openPositions = []
		for txid in result:
			self.openPositions.append({'txid': txid,
						   'type': result[txid]['type'],
						   'pair': result[txid]['pair'],
						   'openVolume': Decimal(result[txid]['vol'])
								-Decimal(result[txid]['vol_closed']),
						   'net': Decimal(result[txid]['net'])})

		print(self.openPositions)

	def GetBalance(self, asset):
		return Decimal(self.balances[asset]) if asset in self.balances else 0

	def GetEquity(self, quote):
		equity = self.GetBalance(quote)
		for position in self.openPositions:
			if self.baseQuoteBreakdownLookup[position['pair']]['quote'] == quote:
				equity = equity + position['net']
		return equity

	def GetOpenPositionBalance(self, base):
		balance = 0
		for position in self.openPositions:
			if self.baseQuoteBreakdownLookup[position['pair']]['base'] == base:
				if position['type'] == 'buy':
					balance = balance + position['openVolume']
				elif position['type'] == 'sell':
					balance = balance - position['openVolume']
		return balance

	def PlaceMarketOrder(self, volume, pair, buyOrSell, leverage = 'none'):
		print("PlaceMarketOrder({}, {}, {}, {})".format(volume, pair, buyOrSell, leverage))
		return
		data = self.k.query_private('AddOrder', data = {'pair': pair,
								'type': buyOrSell,
								'ordertype': 'market',
								'volume': volume,
								'leverage': leverage})
		txids =  data['result']['txid']
		self.WaitUntilNoOpenOrders()
		self.SyncOpenPositions()
		return txids

	def CloseAllOpenPositionsAtMarketPrice(self):
		self.SyncOpenPositions()
		for position in self.openPositions:
			self.ClosePositionAtMarketPrice(position)

	def ClosePositionAtMarketPrice(self, position):
		if position['type'] == 'buy':
			self.PlaceMarketOrder(position['volume'], position['pair'], 'sell', leverage = '2:1')
		elif position['type'] == 'sell':
			self.PlaceMarketOrder(position['volume'], position['pair'], 'buy', leverage = '2:1')
		else:
			raise Exception("Unknown order type: {}".format(position['type']))

	def WaitUntilNoOpenOrders(self):
		while len(self.GetOpenOrderTxids()) > 0:
			time.sleep(1)

	def GetOpenOrderTxids(self):
		return self.TryQueryPrivateUntilSuccess('OpenOrders')['open']

	def CancelOrder(self, txid):
		self.k.query_private('CancelOrder', data = {'txid': txid})

	def CancelAllOpenOrders(self):
		openOrderTxids = self.GetOpenOrderTxids()
		for txid in openOrderTxids:
			self.CancelOrder(txid)

	def GetLeverageOnOrder(self, txid):
		result = self.TryQueryPrivateUntilSuccess('QueryOrders', data = {'txid': txid})
		return result[txid]['descr']['leverage']

	def TryQueryPrivateUntilSuccess(self, query, data=None):
		while True:
			try:
				if data == None:
					data = self.k.query_private(query)
				else:
					data = self.k.query_private(query, data=data)
				if len(data["error"]) > 0:
					raise Exception("API error: {}".format(data["error"]))
				if 'result' not in data:
					raise Exception("Key 'result' not found in data. Data was".format(data))
				return data['result']
			except Exception as error:
				print("Caught error: {}".format(repr(error)))
				sys.stdout.flush()
				time.sleep(1)

	def InitPairs(self):
		self.pairWsNameToBaseQuoteLookup = dict()
		self.baseQuoteToWsnameLookup = dict()
		self.baseQuoteBreakdownLookup = dict()
		data = self.k.query_public('AssetPairs')
		for key, pair in data['result'].items():
			if 'wsname' in pair:
				baseQuote = '{}{}'.format(pair['base'], pair['quote'])
				wsname = pair['wsname']
				self.pairWsNameToBaseQuoteLookup[wsname] = baseQuote
				self.baseQuoteToWsnameLookup[baseQuote] = wsname
				self.baseQuoteBreakdownLookup[baseQuote] = {'base':pair['base'], 'quote':pair['quote']}

		print(self.pairWsNameToBaseQuoteLookup)
		print(self.baseQuoteToWsnameLookup)
		print(self.baseQuoteBreakdownLookup)

if __name__ == "__main__":
	exchangeHandle = ExchangeHandle('kraken.key')

