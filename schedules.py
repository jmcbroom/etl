import schedule
import time
import etl
import arrow

from etl.slack import SlackMessage

msg = SlackMessage({"text": "ETL thread for {}".format(arrow.now().format('dddd, MMMM Do'))})
msg.send()

def run(process, notify=False):
  try:
    p = etl.Process(process)
    p.update()
    if notify:
      msg.comment("Update successful: *{}*".format(process))
  except Exception as e:
    msg.comment("Error: *{}*\n > `{}`".format(process, e))


# Turn this back on once our CAD works.
# schedule.every(5).minutes.do(run, process='angels_night')

schedule.every.day.do(run, process='dlba', notify=True)
schedule.every.day.do(run, process='bseed', notify=True)

while True:
  schedule.run_pending()
  time.sleep(1)
