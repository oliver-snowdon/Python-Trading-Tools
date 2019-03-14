import requests
import json
from Database import Database
import Config

startTimestamp = 1552507774
length = 1

node = Config.nodes[0]

def GetDataSection(startTimestamp, length, node):
	while True:
		r = requests.get('http://{}/runs/{}&{}'.format(node, startTimestamp, startTimestamp+length))
		if r.status_code == 200:
			data = json.loads(r.text)
			print(data)
			if data >= 0:
				run = data
				length = length*2
			else:
				break
		else:
			break

	minLength = int(length/2)
	maxLength = length

	while maxLength-minLength > 1:
		length = int((minLength+maxLength)/2)
		r = requests.get('http://{}/runs/{}&{}'.format(node, startTimestamp, startTimestamp+length))
		if r.status_code == 200:
			data = json.loads(r.text)
			print(data)
			if data >= 0:
				minLength = length
			else:
				maxLength = length
		else:
			break

	print(maxLength)

	url = 'http://{}/trades/{}&XBT.EUR&{}&{}'.format(node, run, startTimestamp, startTimestamp+maxLength)
	print(url)
	r = requests.get(url)
	if r.status_code == 200:
		tradesSection = json.loads(r.text)
	else:
		return None, None

	url = 'http://{}/spreads/{}&XBT.EUR&{}&{}'.format(node, run, startTimestamp, startTimestamp+maxLength)
	print(url)
	r = requests.get(url)
	if r.status_code == 200:
		spreadsSection = json.loads(r.text)
	else:
		return None, None

	return spreadsSection, tradesSection, maxLength

spreadsSection, tradesSection, maxLength = GetDataSection(startTimestamp, length, node)

print(spreadsSection)
print(tradesSection)
