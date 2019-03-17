from Database import Database

database = Database()
cursor = database.cnx.cursor(buffered=True)
cursor.execute('SELECT `id` FROM `runs` WHERE `node` != "localhost";')
for row in cursor:
	print(row[0])
	removeCursor = database.cnx.cursor()
	removeCursor.execute('DELETE FROM `spreads` WHERE `run_id` = %s;', (row[0],))
	removeCursor.execute('DELETE FROM `trades` WHERE `run_id` = %s;', (row[0],))

cursor.execute('DELETE FROM `runs` WHERE `node` != "localhost";')
database.cnx.commit()
