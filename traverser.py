import sqlite3
import math
from datetime import datetime
from datetime import timedelta
from geopy.distance import distance

try:
    conn = sqlite3.connect(':memory:', check_same_thread=False)
except EnvironmentError as e:
    print(e)

date_format = "%Y%m%d"
PRECISION = math.pow(10, -6)
INTERVAL = 7
THRESHOLD = 5

cur = conn.cursor()

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

def contactgraph(subject_id, date):
    contact_graph = set()
    to_be_visited = set(range(1, 12))
    queue = [(subject_id, date)]

    while len(queue) > 0:
        current_subject_id, current_date = queue.pop()
        date_seven_days_ago = (datetime.strptime(date, date_format) + timedelta(days=-INTERVAL)).strftime(date_format)

        if current_subject_id not in to_be_visited:
            continue
        to_be_visited.remove(current_subject_id)
        cur.execute(f'ATTACH DATABASE \"{filename[current_subject_id]}\" AS GS{current_subject_id}')
        for other_subject in to_be_visited:

            cur.execute(f'ATTACH DATABASE \"{filename[other_subject]}\" AS GS{other_subject}')
            query = f'SELECT GS{other_subject}.locationTable._time_location, GS{current_subject_id}.locationTable._latitude, GS{current_subject_id}.locationTable._longitude, GS{other_subject}.locationTable._latitude, GS{other_subject}.locationTable._longitude FROM GS{current_subject_id}.locationTable JOIN GS{other_subject}.locationTable WHERE GS{current_subject_id}.locationTable._time_location <= \"{current_date}\" AND GS{current_subject_id}.locationTable._time_location >= \"{date_seven_days_ago}\" ;'
            cur.execute(query)
            for row in cur.fetchall():
                if((distance((row[1]*PRECISION, row[2]*PRECISION), (row[3]*PRECISION, row[4]*PRECISION)).km) >= THRESHOLD):
                    queue.append((other_subject, row[0]))
                    contact_graph.add((current_subject_id, other_subject))
                    break
            cur.execute(f'DETACH DATABASE GS{other_subject}')
        cur.execute(f'DETACH DATABASE GS{current_subject_id}')
    return contact_graph

def as_txt_file(contact_graph_edges):
    filename = 'contact_graph.txt'
    file = open(filename, 'w')
    for x in range(1, 12):
        for y in range(1, 12):
            if((x, y) in contact_graph_edges or (y, x) in contact_graph_edges):
                file.write('1 ')
            else:
                file.write('0 ')
        file.write('\n')
    file.close()
    return file