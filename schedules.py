import schedule
import time
import etl
import arrow

from etl.slack import SlackMessage

this_day = arrow.now().format('dddd, MMMM Do')
msg = SlackMessage({"text": "ETL thread for {}".format(this_day)})
msg.send()

def run(process, dataset=None, notify=False):
  try:
    p = etl.Process(process)
    p.update(dataset)
    if notify:
      msg.comment("Update successful: *{}*".format(process))
  except Exception as e:
    msg.comment("Error: *{}*\n > `{}`".format(process, e))


## Scheduling datasets

# Angel's Night fire data
schedule.every(5).minutes.do(run, process='angels_night')
# DLBA datasets (7 in total?)
schedule.every().day.at("9:00").do(run, process='dlba', notify=True)
# BSEED - just permits for now
schedule.every().day.at("9:15").do(run, process='bseed', dataset="Building Permits", notify=True)
# CAD
schedule.every().day.at.("2:00").do(run, process='cad', notify=True)

# infinite loop
while True:
  # if it's the next day
  if arrow.now().format('dddd, MMMM Do') != this_day:
    # reset this_day to today
    this_day = arrow.now().format('dddd, MMMM Do')
    # create and send a new SlackMsg
    msg = SlackMessage({"text": "ETL thread for {}".format(this_day)})
    msg.send()

  # run whatever is on deck
  schedule.run_pending()

  # wait a minute before checking again
  time.sleep(60)
