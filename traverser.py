import sqlite3
import math
from datetime import datetime
from datetime import timedelta
from geopy.distance import distance

try:
    conn = sqlite3.connect(':memory:')
except EnvironmentError as e:
    print(e)

date_format = "%Y%m%d%H%M%S%a"
PRECISION = math.pow(10, -6)

date = '20120813113014SAT'
date_seven_days_ago = (datetime.strptime(date, date_format) + timedelta(days=-1000)).strftime(date_format)

cur = conn.cursor()

databases = [
    { 'id' : 1, 'filename' : "LifeMap_GS1.db" },
    { 'id' : 2, 'filename' : "LifeMap_GS2.db" },
]

def attach(databases):
    for database in databases:
        subject_id = database['id']
        filename = database['filename']
        cur.execute(f'ATTACH DATABASE \"{filename}\" AS GS{subject_id}')

attach(databases = databases)

query = f'SELECT GS1.locationTable._time_location, GS1.locationTable._latitude, GS1.locationTable._longitude, GS2.locationTable._latitude, GS2.locationTable._longitude FROM GS1.locationTable JOIN GS2.locationTable ON GS1.locationTable._time_location = GS2.locationTable._time_location WHERE GS1.locationTable._time_location <= \"{date}\" AND GS1.locationTable._time_location >= \"{date_seven_days_ago}\" ;'

cur.execute(query)

for row in cur.fetchall():
    print(distance((row[1]*PRECISION, row[2]*PRECISION), (row[3]*PRECISION, row[4]*PRECISION)).km)