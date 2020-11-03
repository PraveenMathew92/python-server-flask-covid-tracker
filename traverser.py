import sqlite3
import math
from datetime import datetime
from datetime import timedelta
from geopy.distance import distance

try:
    conn = sqlite3.connect('LifeMap_GS4.db')
except EnvironmentError as e:
    print(e)

date_format = "%Y%m%d%H%M%S%a"
PRECISION = math.pow(10, -6)

date = '20120813113014SAT'
date_seven_days_ago = (datetime.strptime(date, date_format) + timedelta(days=-1000)).strftime(date_format)

cur = conn.cursor()
cur.execute('ATTACH DATABASE "LifeMap_GS2.db" AS GS2')

query = f'SELECT main.locationTable._time_location, main.locationTable._latitude, main.locationTable._longitude, GS2.locationTable._latitude, GS2.locationTable._longitude FROM main.locationTable JOIN GS2.locationTable ON main.locationTable._time_location = GS2.locationTable._time_location WHERE main.locationTable._time_location <= \"{date}\" AND main.locationTable._time_location >= \"{date_seven_days_ago}\" ;'

cur.execute(query)

for row in cur.fetchall():
    print(distance((row[1]*PRECISION, row[2]*PRECISION), (row[3]*PRECISION, row[4]*PRECISION)).km)