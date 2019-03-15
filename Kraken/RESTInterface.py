import sys
import os
import os.path

thisFile = os.path.realpath(__file__)
directory = os.path.dirname(thisFile)

sys.path.append(os.path.join(directory, 'python3-krakenex'))

import krakenex

def ConvertPairFormat(pair):
	pz = pair.replace('/', 'Z', 1)
	return 'X{}'.format(pz)

def GetTrades(pair, startTimestamp):
	k = krakenex.API()
	return k.query_public('Trades', data = {'pair': ConvertPairFormat(pair), 'since': startTimestamp})

def GetPairs():
	k = krakenex.API()
	data = k.query_public('AssetPairs')
	pairs = []
	for pair, pairInfo in data['result'].items():
		if 'wsname' in pairInfo:
			pairs.append(pairInfo['wsname'])
	return pairs


if __name__ == "__main__":
	print(GetPairs())
