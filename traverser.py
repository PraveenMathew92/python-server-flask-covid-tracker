import sqlite3
from datetime import datetime
from datetime import timedelta

try:
    conn = sqlite3.connect('LifeMap_GS1.db')
except EnvironmentError as e:
    print(e)

date = '20110813113014SAT'
date_format = "%Y%m%d%H%M%S%a"
date_seven_days_ago = (datetime.strptime(date, date_format) + timedelta(days=-7)).strftime(date_format)

print(date_seven_days_ago)
cur = conn.cursor()

query = f'SELECT _time_location, _latitude, _longitude FROM locationTable WHERE _time_location <= \"{date}\" AND _time_location >= \"{date_seven_days_ago}\" ;'
print(query)
cur.execute(query)

for row in cur.fetchall():
    print(row)