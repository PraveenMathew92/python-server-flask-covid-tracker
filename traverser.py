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
INTERVAL = 1000
THRESHOLD = 5

date = '2011114113014MON'
date_seven_days_ago = (datetime.strptime(date, date_format) + timedelta(days=-INTERVAL)).strftime(date_format)

cur = conn.cursor()

databases = [
    { 'id' : 1, 'filename' : "LifeMap_GS1.db" },
    { 'id' : 2, 'filename' : "LifeMap_GS2.db" },
    { 'id' : 3, 'filename' : "LifeMap_GS3.db" },
    { 'id' : 4, 'filename' : "LifeMap_GS4.db" },
    { 'id' : 5, 'filename' : "LifeMap_GS5.db" },
    { 'id' : 6, 'filename' : "LifeMap_GS6.db" },
    { 'id' : 7, 'filename' : "LifeMap_GS7.db" },
    { 'id' : 8, 'filename' : "LifeMap_GS8.db" },
    { 'id' : 9, 'filename' : "LifeMap_GS9.db" },
    { 'id' : 10, 'filename' : "LifeMap_GS10.db" },
    { 'id' : 11, 'filename' : "LifeMap_GS11.db" },
]

filename = [
    "",
    "LifeMap_GS1.db",
    "LifeMap_GS2.db",
    "LifeMap_GS3.db",
    "LifeMap_GS4.db",
    "LifeMap_GS5.db",
    "LifeMap_GS6.db",
    "LifeMap_GS7.db",
    "LifeMap_GS8.db",
    "LifeMap_GS9.db",
    "LifeMap_GS10.db",
    "LifeMap_GS11.db",
]

def attach(databases):
    for database in databases:
        subject_id = database['id']
        filename = database['filename']
        cur.execute(f'ATTACH DATABASE \"{filename}\" AS GS{subject_id}')

# attach(databases = databases)

def contactgraph(subject_id, date):
    contact_graph = set()
    to_be_visited = set(range(1, 12))
    queue = [(subject_id, date)]
    # print(to_be_visited)
    while len(queue) > 0:
        current_subject_id, current_date = queue.pop()
        # print("CURRENT : ", current_subject_id)
        if current_subject_id not in to_be_visited:
            continue
        to_be_visited.remove(current_subject_id)
        cur.execute(f'ATTACH DATABASE \"{filename[current_subject_id]}\" AS GS{current_subject_id}')
        for other_subject in to_be_visited:
            # print("SUBJECT", other_subject)
            cur.execute(f'ATTACH DATABASE \"{filename[other_subject]}\" AS GS{other_subject}')
            query = f'SELECT GS{current_subject_id}.locationTable._time_location, GS{current_subject_id}.locationTable._latitude, GS{current_subject_id}.locationTable._longitude, GS{other_subject}.locationTable._latitude, GS{other_subject}.locationTable._longitude FROM GS{current_subject_id}.locationTable JOIN GS{other_subject}.locationTable ON GS{current_subject_id}.locationTable._time_location = GS{other_subject}.locationTable._time_location WHERE GS{current_subject_id}.locationTable._time_location <= \"{date}\" AND GS{current_subject_id}.locationTable._time_location >= \"{date_seven_days_ago}\" ;'
            cur.execute(query)
            for row in cur.fetchall():
                if((distance((row[1]*PRECISION, row[2]*PRECISION), (row[3]*PRECISION, row[4]*PRECISION)).km) >= THRESHOLD):
                    queue.append((other_subject, row[0]))
                    contact_graph.add((current_subject_id, other_subject))
                    # contact_graph[set(other_subject, subject_id)] = 1
                    break
            cur.execute(f'DETACH DATABASE GS{other_subject}')
        cur.execute(f'DETACH DATABASE GS{current_subject_id}')
    return contact_graph

# query = f'SELECT GS1.locationTable._time_location, GS1.locationTable._latitude, GS1.locationTable._longitude, GS2.locationTable._latitude, GS2.locationTable._longitude FROM GS1.locationTable JOIN GS2.locationTable ON GS1.locationTable._time_location = GS2.locationTable._time_location WHERE GS1.locationTable._time_location <= \"{date}\" AND GS1.locationTable._time_location >= \"{date_seven_days_ago}\" ;'

# cur.execute(query)

for i in range(1, 12):
    print("SUBJECT: ", i)
    row = contactgraph(i, date)
    for x in range(1, 12):
        for y in range(1, 12):
            if((x, y) in row or (y, x) in row):
                print("1", end =" ")
            else:
                print("0", end =" ")
        print()

for row in cur.fetchall():
    print(distance((row[1]*PRECISION, row[2]*PRECISION), (row[3]*PRECISION, row[4]*PRECISION)).km)