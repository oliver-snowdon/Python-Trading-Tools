from flask import Flask, jsonify, abort
from Database import Database

pairs = ["XBT/EUR", "ETH/EUR"]

app = Flask(__name__)

@app.route('/spreads/count/<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetSpreadCount(pair, timestampStart, timestampUpTo):
	database = Database()
	pairIds = database.GetPairIds(pairs)
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.CountSpreads(pairIds[pair], timestampStart, timestampUpTo))

@app.route('/trades/count/<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetTradeCount(pair, timestampStart, timestampUpTo):
	database = Database()
	pairIds = database.GetPairIds(pairs)
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.CountTrades(pairIds[pair], timestampStart, timestampUpTo))

@app.route('/spreads/<int:runId>&<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetSpreads(runId, pair, timestampStart, timestampUpTo):
	database = Database()
	pairIds = database.GetPairIds(pairs)
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.GetSpreads(runId, pairIds[pair], timestampStart, timestampUpTo))

@app.route('/trades/<int:runId>&<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetTrades(runId, pair, timestampStart, timestampUpTo):
	database = Database()
	pairIds = database.GetPairIds(pairs)
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.GetTrades(runId, pairIds[pair], timestampStart, timestampUpTo))

@app.route('/runs/<int:interruptionStart>&<int:interruptionEnd>', methods=['GET'])
def GetLocalOverlappingRun(interruptionStart, interruptionEnd):
	database = Database()
	if interruptionStart >= interruptionEnd:
		abort(404)
	return jsonify(database.GetLocalOverlappingRun(interruptionStart, interruptionEnd))

@app.route('/runs', methods=['GET'])
def GetNonEmptyLocalRuns():
	database = Database()
	result = database.GetNonEmptyLocalRuns()
	print(result)
	return jsonify(result)

# https://stackoverflow.com/questions/34066804/disabling-caching-in-flask
@app.after_request
def AddHeader(r):
	"""
	Add headers to both force latest IE rendering engine or Chrome Frame,
	and also to cache the rendered page for 10 minutes.
	"""
	r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
	r.headers["Pragma"] = "no-cache"
	r.headers["Expires"] = "0"
	r.headers['Cache-Control'] = 'public, max-age=0'
	return r

if __name__ == '__main__':
	app.run(debug=True, port=8080, host='0.0.0.0')
