if __name__ == "__main__" and __package__ is None:
    from sys import path
    from os.path import dirname as dir

    path.append(dir(path[0]))
    __package__ = "slack"

import os
from flask import Flask, request, Response
import threading
app = Flask(__name__)

SLACK_WEBHOOK_SECRET = os.environ['SLACK_WEBHOOK']

# /geocode [Sheet ID] [name of address column]
from smartsheets import GeocodeSheet, SheetToSocrata
@app.route('/ss-geocode', methods=['POST'])
def geocode_ss():
    vals = request.form.get('text').split(' ')
    sheet_id = vals[0]
    add_col = vals[1]
    ss = GeocodeSheet(sheet_id=sheet_id, address_col=add_col)
    geocode_thread = threading.Thread(target=ss.geocode_rows)
    geocode_thread.start()
    return Response(), 200

# /ss-socrata [Sheet ID] [Socrata 4x4]
@app.route('/ss-socrata', methods=['POST'])
def ss_to_socrata():
    vals = request.form.get('text').split(' ')
    sheet_id = vals[0]
    add_col = vals[1]
    ss = SheetToSocrata(sheet_id=sheet_id, socrata_4x4=add_col)
    geocode_thread = threading.Thread(target=ss.geocode_rows)
    geocode_thread.start()
    return Response(), 200

# /ss-ago [Sheet ID]
@app.route('/ss-ago', methods=['POST'])
def ss_to_ago():
    vals = request.form.get('text').split(' ')
    sheet_id = vals[0]
    add_col = vals[1]
    ss = SheetToAGO(sheet_id=sheet_id)
    geocode_thread = threading.Thread(target=ss.geocode_rows)
    geocode_thread.start()
    return Response(), 200

if __name__ == "__main__":
    app.run(debug=True)
