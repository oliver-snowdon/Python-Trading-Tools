import json
import requests
from decimal import Decimal
from Database import Database
from Kraken import RESTInterface
import Config
import numpy as np
import math

def FindBestCoveringRun(runs, pair, firstEvent, lastEvent):
	coveringRuns = []
	usableLengths = []
	for run in runs:
		if pair in run["pairs"] and run["firstTimestamp"] < firstEvent and run["lastTimestamp"] > lastEvent:
			coveringRuns.append(run)
			usableLengths.append(run["lastTimestamp"] - firstEvent)
	if len(coveringRuns) > 0:
		return coveringRuns[np.argsort(usableLengths)[-1]]
		
	for run in runs:
		if pair in run["pairs"] and run["firstTimestamp"] <= firstEvent and run["lastTimestamp"] >= lastEvent:
			coveringRuns.append(run)
			usableLengths.append(run["lastTimestamp"] - firstEvent)
	if len(coveringRuns) > 0:
		return coveringRuns[np.argsort(usableLengths)[-1]]
		
	return None

def FindCoveringRunInclusive(runs, pair, firstEvent, lastEvent):
	for run in runs:
		if pair in run["pairs"] and run["firstTimestamp"] <= firstEvent and run["lastTimestamp"] >= lastEvent:
			return run
	return None

def FillGap(database, pair, start, end, remoteRuns):
	print("start = {}, end = {}".format(start, end))
	database.cnx.commit()
	cursor = database.cnx.cursor()
	cursor.execute("SELECT `id`, `first_timestamp`, `last_timestamp`, `pairs` FROM `runs`")
	eventTimestamps = set()
	eventTimestamps.add(start)
	eventTimestamps.add(end)
	runs = list()
	for row in cursor:
		if pair in json.loads(row[3]):
			firstTimestamp = Decimal(row[1])
			lastTimestamp = Decimal(row[2])
			if firstTimestamp<=lastTimestamp:
				if firstTimestamp > start and firstTimestamp < end:
					eventTimestamps.add(firstTimestamp)
				if lastTimestamp < end and lastTimestamp > start:
					eventTimestamps.add(lastTimestamp)
				runs.append({"id":row[0], "firstTimestamp":firstTimestamp, "lastTimestamp":lastTimestamp,
					     "pairs":json.loads(row[3])})
	sortedEventTimestamps = sorted(eventTimestamps)
	print(sortedEventTimestamps)
	for i in range(len(sortedEventTimestamps)-1):
		assert(sortedEventTimestamps[i] >= start and sortedEventTimestamps[i] <= end)
		assert(sortedEventTimestamps[i+1] >= start and sortedEventTimestamps[i+1] <= end)
		coveringRun = FindCoveringRunInclusive(runs, pair, sortedEventTimestamps[i], sortedEventTimestamps[i+1])
		if coveringRun == None:
			remoteEventTimestamps = set()
			remoteEventTimestamps.add(sortedEventTimestamps[i])
			remoteEventTimestamps.add(sortedEventTimestamps[i+1])
			for run in remoteRuns:
				if run['firstTimestamp'] > sortedEventTimestamps[i] and run['firstTimestamp'] < sortedEventTimestamps[i+1]:
					remoteEventTimestamps.add(run['firstTimestamp'])
				if run['lastTimestamp'] > sortedEventTimestamps[i] and run['lastTimestamp'] < sortedEventTimestamps[i+1]:
					remoteEventTimestamps.add(run['lastTimestamp'])
			sortedRemoteEventTimestamps = sorted(remoteEventTimestamps)

			filledUpTo = sortedEventTimestamps[i]
			
			for j in range(len(sortedRemoteEventTimestamps)-1):
				print("sortedRemoteEventTimestamps")
				print(sortedRemoteEventTimestamps[j])
				print(sortedRemoteEventTimestamps[j+1])
				
				if sortedRemoteEventTimestamps[j] >= filledUpTo:
					remoteCoveringRun = FindBestCoveringRun(remoteRuns, pair, sortedRemoteEventTimestamps[j], sortedRemoteEventTimestamps[j+1])

					print(remoteCoveringRun)
						
					if remoteCoveringRun != None and remoteCoveringRun['lastTimestamp']:
						filledUpTo = AddRemoteRun(remoteCoveringRun["node"], remoteCoveringRun["remoteRunId"],
									  sortedRemoteEventTimestamps[j], sortedEventTimestamps[i+1]+1, pair, database)
				
		else:
			print(coveringRun)

def DownloadRuns(pair):
	result = list()
	for node in Config.nodes:
		r = requests.get('http://{}/runs'.format(node))
		if r.status_code == 200:
			data = json.loads(r.text)
			for run in data:
				if pair in run["pairs"]:
					result.append({"node":node, "remoteRunId":run["id"], "pairs":run["pairs"],
						       "firstTimestamp":Decimal(run["start"]), "lastTimestamp":Decimal(run["end"])})
	return result

def AddRemoteRun(node, remoteRunId, startTimestamp, endTimestamp, pair, database):
	assert(startTimestamp < endTimestamp)
	pairIds = database.GetPairIds([pair])
	url = 'http://{}/runs'.format(node)
	r = requests.get(url)
	if r.status_code != 200:
		raise Exception("Could not download {}".format(url))
	data = json.loads(r.text)
	for run in data:
		if remoteRunId == run["id"]:
			assert(Decimal(run["start"]) < Decimal(run["end"]))
			firstTimestampToDownload = int(max(Decimal(run["start"]), startTimestamp))
			lastTimestampToDownload = int(min(Decimal(run["end"]), endTimestamp))
			if firstTimestampToDownload == lastTimestampToDownload:
				return lastTimestampToDownload
			assert(firstTimestampToDownload < lastTimestampToDownload)
			cursor = database.cnx.cursor()
			maxLengthToDownload = 3600
			nSections = int(math.ceil((lastTimestampToDownload-firstTimestampToDownload)/maxLengthToDownload))
			for i in range(nSections):
				firstTimestampInSection = firstTimestampToDownload + i * maxLengthToDownload
				lastTimestampInSection = firstTimestampToDownload + (i+1) * maxLengthToDownload
				if lastTimestampInSection > lastTimestampToDownload:
					lastTimestampInSection = lastTimestampToDownload
				url = 'http://{}/spreads/{}&{}&{}&{}'.format(node, remoteRunId, pair.replace('/', '.'), firstTimestampInSection, lastTimestampInSection)
				print(url)
				r = requests.get(url)
				if r.status_code != 200:
					raise Exception("Could not download {}".format(url))
				spreads = json.loads(r.text)
				#print(spreads)
				url = 'http://{}/trades/{}&{}&{}&{}'.format(node, remoteRunId, pair.replace('/', '.'), firstTimestampInSection, lastTimestampInSection)
				print(url)
				r = requests.get(url)
				if r.status_code != 200:
					raise Exception("Could not download {}".format(url))
				trades = json.loads(r.text)
				#print(trades)

				if i == 0:
					cursor.execute("INSERT INTO `runs` (`node`, `remote_run_id`, `pairs`, `first_timestamp`, `last_timestamp`, `error`) VALUES (%s, %s, %s, %s, %s, %s);", (node, remoteRunId, json.dumps([pair]), firstTimestampToDownload, lastTimestampInSection, ""))
					insertedRunId = cursor.lastrowid
				else:
					cursor.execute("UPDATE `runs` SET `last_timestamp` = %s WHERE `id` = %s", (lastTimestampInSection, insertedRunId))

				for spread in spreads:
					cursor.execute("INSERT INTO `spreads` (`run_id`, `pair_id`, `ask`, `bid`, `timestamp`) VALUES (%s, %s, %s, %s, %s);", (insertedRunId, pairIds[pair], spread["ask"], spread["bid"], spread["timestamp"]))
				for trade in trades:
					cursor.execute("INSERT INTO `trades` (`run_id`, `pair_id`, `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (insertedRunId, pairIds[pair], trade["price"], trade["amount"], trade["timestamp"], trade["buy_or_sell"], trade["market_or_limit"], trade["misc"]))
				database.cnx.commit()
				if i == nSections-1:
					assert(lastTimestampInSection == lastTimestampToDownload)
			return lastTimestampToDownload
	raise Exception("Could not find run {}.{}".format(node, remoteRunId))

if __name__ == "__main__":
	pairs = RESTInterface.GetPairs()
	for pair in pairs:
		remoteRuns = DownloadRuns(pair)
		FillGap(Database(), pair, 0, 1000000000000000, remoteRuns)
