from flask import Flask, jsonify, abort
from Database import Database

pairs = ["GNO/USD", "EOS/EUR", "XRP/USD", "QTUM/EUR", "MLN/ETH", "ETH/JPY", "XBT/JPY", "ETH/GBP", "MLN/XBT", "XLM/XBT", "ADA/USD", "XRP/CAD", "LTC/USD", "XBT/CAD", "ZEC/XBT", "USDT/USD", "DASH/XBT", "XRP/XBT", "EOS/XBT", "REP/ETH", "XRP/EUR", "XLM/EUR", "XMR/XBT", "ADA/ETH", "GNO/XBT", "GNO/EUR", "BSV/XBT", "XTZ/XBT", "QTUM/USD", "ETH/CAD", "ZEC/USD", "BCH/EUR", "XBT/GBP", "ETC/ETH", "ADA/CAD", "QTUM/ETH", "XDG/XBT", "XTZ/USD", "REP/USD", "XBT/USD", "QTUM/CAD", "XLM/USD", "ZEC/JPY", "DASH/USD", "BCH/USD", "XTZ/CAD", "XRP/JPY", "ADA/XBT", "EOS/ETH", "BSV/USD", "ETH/EUR", "REP/EUR", "LTC/XBT", "XBT/EUR", "XMR/EUR", "ETC/EUR", "ADA/EUR", "QTUM/XBT", "ETH/XBT", "EOS/USD", "ETH/USD", "XTZ/ETH", "ETC/USD", "ZEC/EUR", "REP/XBT", "XMR/USD", "DASH/EUR", "XTZ/EUR", "BCH/XBT", "GNO/ETH", "ETC/XBT", "BSV/EUR", "LTC/EUR"]

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
def GetNonEmptyRuns():
	database = Database()
	result = database.GetNonEmptyRuns()
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
