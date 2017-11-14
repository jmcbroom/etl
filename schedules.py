import schedule
import time
import etl

from etl.slack import SlackMessage

msg = SlackMessage({"text": "ETL thread for 2017/11/13"})
msg.send()

def run(process, notify=False):
  try:
    p = etl.Process(process)
    p.update()
    if notify:
      msg.comment("Update successful: *{}*".format(process))
  except Exception as e:
    msg.comment("Error: *{}*\n > `{}`".format(process, e))


schedule.every(5).minutes.do(run, process='angels_night')
# schedule.every.day.do(run, process='bseed', notify=True)
# schedule.every.tuesday.do(run('medical_marijuana'))

while True:
  schedule.run_pending()
  time.sleep(1)
