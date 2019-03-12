import numpy as np

keepFiatIfCan = 0
buyIfCan = 1
keepAssetIfCan = 2
sellIfCan = 3

def RetrospectiveOptimum(ask, bid, exchangeFee, nSamples):

	Qs = np.zeros((4, nSamples))

	for i in reversed(range(nSamples-1)):

		Qs[buyIfCan, i] = np.log((1-exchangeFee) * bid[i+1]/ask[i]) + max(Qs[keepAssetIfCan, i+1], Qs[sellIfCan, i+1])
		Qs[keepFiatIfCan, i] = max(Qs[keepFiatIfCan, i+1], Qs[buyIfCan, i+1])
		Qs[sellIfCan, i] = np.log(1-exchangeFee) + max(Qs[keepFiatIfCan, i+1], Qs[buyIfCan, i+1])
		Qs[keepAssetIfCan, i] = np.log(bid[i+1]/bid[i]) + max(Qs[keepAssetIfCan, i+1], Qs[sellIfCan, i+1])

	return np.argmax(Qs[0:2, 0:nSamples-1], axis=0), np.argmax(Qs[2:4, 0:nSamples-1], axis=0)

