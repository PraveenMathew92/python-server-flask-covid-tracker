from flask import Flask, send_file
from markupsafe import escape
from traverser import contactgraph, as_txt_file

app = Flask(__name__)

@app.route('/<int:subject_id>/<string:date>')
def hello_world(subject_id, date):
    try:
        response_file = open(as_txt_file(contactgraph(subject_id, date)).name, 'r')
        for line in response_file.readlines:
            print(line)
        return send_file(response_file, as_attachment=True, attachment_filename='adjacency_matrix.txt')
    except Exception as e:
        return str(e)