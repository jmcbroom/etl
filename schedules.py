import schedule
import time
import etl

from etl.slack import SlackMessage

msg = SlackMessage({"text": "ETL thread for 2017/11/13"})
msg.send()

def run(process):
  p = etl.Process(process)
  try:
    p.update()
  except Error:
    msg.comment("Error on {}".format(process))


schedule.every(5).minutes.do(run, process='angels_night')
# schedule.every.tuesday.do(run('medical_marijuana'))

while True:
    schedule.run_pending()
    time.sleep(1)
