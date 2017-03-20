if __name__ == "__main__" and __package__ is None:
    from sys import path
    from os.path import dirname as dir

    path.append(dir(path[0]))
    __package__ = "slack"

from smartsheets import EnrichSheetAddresses
import os
from flask import Flask, request, Response
import threading
app = Flask(__name__)

SLACK_WEBHOOK_SECRET = os.environ['SLACK_WEBHOOK']

@app.route('/', methods=['POST'])
def inbound():
    vals = request.form.get('text').split(' ')
    sheet_id = vals[0]
    add_col = vals[1]
    print(sheet_id)
    print(add_col)
    ss = EnrichSheetAddresses(sheet_id=sheet_id, address_col=add_col)
    geocode_thread = threading.Thread(target=ss.geocode_rows)
    geocode_thread.start()
    return Response(), 200
    # if request.form.get('token') == SLACK_WEBHOOK_SECRET:
    #     channel = request.form.get('channel_name')
    #     username = request.form.get('user_name')
    #     text = request.form.get('text')
    #     inbound_message = username + " in " + channel + " says: " + text
    #     print(inbound_message)
    # return Response(), 200


# @app.route('/', methods=['GET'])
# def test():
#     return Response('It works!')


if __name__ == "__main__":
    app.run(debug=True)
