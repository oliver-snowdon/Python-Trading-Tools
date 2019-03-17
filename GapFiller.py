import json
import requests
from Database import Database
import Config

def FindCoveringRun(runs, pair, firstEvent, lastEvent):
	for run in runs:
		if pair in run["pairs"] and run["firstTimestamp"] < firstEvent and run["lastTimestamp"] > lastEvent:
			return run
	return None

def FillGap(database, pair, start, end):
	remoteRuns = DownloadRuns()
	print(remoteRuns)
	cursor = database.cnx.cursor()
	cursor.execute("SELECT `id`, `first_timestamp`, `last_timestamp`, `pairs` FROM `runs`")
	eventTimestamps = set()
	eventTimestamps.add(start)
	eventTimestamps.add(end)
	runs = list()
	for row in cursor:
		if pair in json.loads(row[3]):
			if (row[1] > start):
				eventTimestamps.add(row[1])
			if (row[2] < end):
				eventTimestamps.add(row[2])
			runs.append({"id":row[0], "firstTimestamp":row[1], "lastTimestamp":row[2], "pairs":json.loads(row[3])})
	sortedEventTimestamps = sorted(eventTimestamps)
	for i in range(len(sortedEventTimestamps)-1):
		coveringRun = FindCoveringRun(runs, pair, sortedEventTimestamps[i], sortedEventTimestamps[i+1])
		if coveringRun == None:
			remoteCoveringRun = FindCoveringRun(remoteRuns, pair, sortedEventTimestamps[i], sortedEventTimestamps[i+1])
			print(remoteCoveringRun)
		print(sortedEventTimestamps[i])
		print(sortedEventTimestamps[i+1])

def DownloadRuns():
	result = list()
	for node in Config.nodes:
		r = requests.get('http://{}/runs'.format(node))
		if r.status_code == 200:
			data = json.loads(r.text)
			for run in data:
				result.append({"node":node, "remoteRunId":run["id"], "pairs":run["pairs"],
					       "firstTimestamp":run["start"], "lastTimestamp":run["end"]})
	return result

FillGap(Database(), "ETH/EUR", 0, 1000000000000000)
