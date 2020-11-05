import os
from werkzeug.utils import secure_filename
from flask import Flask, request, send_file
from markupsafe import escape
from traverser import contactgraph, as_txt_file

app = Flask(__name__)

@app.route('/<int:subject_id>/<string:date>')
def hello_world(subject_id, date):
    try:
        response_file = open(as_txt_file(contactgraph(subject_id, date)).name, 'rb')
        return send_file(response_file, as_attachment=True, attachment_filename='adjacency_matrix.txt')
    except Exception as e:
        return str(e)

@app.route('/uploadfile', methods=['POST'])
def upload_file():
    print(str(request.files))
    if 'file' not in request.files:
        return 'no file'
    file = request.files['file']
    if file.filename == '':
        return 'no filename'
    else:
        filename = secure_filename(file.filename)
        file.save(filename)
        return 'saved file successfully'