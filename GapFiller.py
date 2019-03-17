import json
import requests
from decimal import Decimal
from Database import Database
from Kraken import RESTInterface
import Config

def FindCoveringRun(runs, pair, firstEvent, lastEvent):
	for run in runs:
		if pair in run["pairs"] and run["firstTimestamp"] < firstEvent and run["lastTimestamp"] > lastEvent:
			return run
	return None

def FindCoveringRunInclusive(runs, pair, firstEvent, lastEvent):
	for run in runs:
		if pair in run["pairs"] and run["firstTimestamp"] <= firstEvent and run["lastTimestamp"] >= lastEvent:
			return run
	return None

def FillGap(database, pair, start, end, remoteRuns):
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
			assert(firstTimestamp<=lastTimestamp)
			if firstTimestamp > start:
				eventTimestamps.add(firstTimestamp)
			if lastTimestamp < end:
				eventTimestamps.add(lastTimestamp)
			runs.append({"id":row[0], "firstTimestamp":firstTimestamp, "lastTimestamp":lastTimestamp,
				     "pairs":json.loads(row[3])})
	sortedEventTimestamps = sorted(eventTimestamps)
	for i in range(len(sortedEventTimestamps)-1):
		coveringRun = FindCoveringRunInclusive(runs, pair, sortedEventTimestamps[i], sortedEventTimestamps[i+1])
		if coveringRun == None:
			remoteCoveringRun = FindCoveringRun(remoteRuns, pair, sortedEventTimestamps[i], sortedEventTimestamps[i+1])

			print(remoteCoveringRun)
				
			if remoteCoveringRun != None:
				AddRemoteRun(remoteCoveringRun["node"], remoteCoveringRun["remoteRunId"],
					     sortedEventTimestamps[i]-1, sortedEventTimestamps[i+1]+1, pair, database)
				
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
	pairIds = database.GetPairIds([pair])
	url = 'http://{}/runs'.format(node)
	r = requests.get(url)
	if r.status_code != 200:
		raise Exception("Could not download {}".format(url))
	data = json.loads(r.text)
	for run in data:
		if remoteRunId == run["id"]:
			firstTimestampToDownload = int(max(Decimal(run["start"]), startTimestamp))
			lastTimestampToDownload = int(min(Decimal(run["end"]), endTimestamp))
			url = 'http://{}/spreads/{}&{}&{}&{}'.format(node, remoteRunId, pair.replace('/', '.'), firstTimestampToDownload, lastTimestampToDownload)
			print(url)
			r = requests.get(url)
			if r.status_code != 200:
				raise Exception("Could not download {}".format(url))
			spreads = json.loads(r.text)
			print(spreads)
			url = 'http://{}/trades/{}&{}&{}&{}'.format(node, remoteRunId, pair.replace('/', '.'), firstTimestampToDownload, lastTimestampToDownload)
			print(url)
			r = requests.get(url)
			if r.status_code != 200:
				raise Exception("Could not download {}".format(url))
			trades = json.loads(r.text)
			print(trades)
			cursor = database.cnx.cursor()
			cursor.execute("INSERT INTO `runs` (`node`, `remote_run_id`, `pairs`, `first_timestamp`, `last_timestamp`, `error`) VALUES (%s, %s, %s, %s, %s, %s);", (node, remoteRunId, json.dumps([pair]), firstTimestampToDownload, lastTimestampToDownload, ""))
			database.cnx.commit()
			insertedRunId = cursor.lastrowid
			for spread in spreads:
				cursor.execute("INSERT INTO `spreads` (`run_id`, `pair_id`, `ask`, `bid`, `timestamp`) VALUES (%s, %s, %s, %s, %s);", (insertedRunId, pairIds[pair], spread["ask"], spread["bid"], spread["timestamp"]))
			for trade in trades:
				cursor.execute("INSERT INTO `trades` (`run_id`, `pair_id`, `price`, `amount`, `timestamp`, `buy_or_sell`, `market_or_limit`, `misc`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (insertedRunId, pairIds[pair], trade["price"], trade["amount"], trade["timestamp"], trade["buy_or_sell"], trade["market_or_limit"], trade["misc"]))
			database.cnx.commit()

pairs = RESTInterface.GetPairs()
for pair in pairs:
	remoteRuns = DownloadRuns(pair)
	FillGap(Database(), pair, 0, 1000000000000000, remoteRuns)
