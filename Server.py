from flask import Flask, jsonify, abort
from Database import Database

pairs = ["XBT/EUR", "ETH/EUR"]

app = Flask(__name__)
database = Database()
pairIds = database.GetPairIds(pairs)

@app.route('/spreads/count/<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetSpreadCount(pair, timestampStart, timestampUpTo):
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.CountSpreads(pairIds[pair], timestampStart, timestampUpTo))

@app.route('/trades/count/<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetTradeCount(pair, timestampStart, timestampUpTo):
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.CountTrades(pairIds[pair], timestampStart, timestampUpTo))

@app.route('/spreads/<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetSpreads(pair, timestampStart, timestampUpTo):
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.GetSpreads(pairIds[pair], timestampStart, timestampUpTo))

@app.route('/trades/<string:pair>&<int:timestampStart>&<int:timestampUpTo>', methods=['GET'])
def GetTrades(pair, timestampStart, timestampUpTo):
	pair = pair.replace('.', '/')
	if pair not in pairIds:
		abort(404)
	if timestampStart >= timestampUpTo:
		abort(404)
	return jsonify(database.GetTrades(pairIds[pair], timestampStart, timestampUpTo))

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
	app.run(debug=True, port=8080)
