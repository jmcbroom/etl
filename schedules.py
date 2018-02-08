import time
import etl
import arrow
import socket

from etl.slack import SlackMessage

this_day = arrow.now().format('dddd, MMMM Do')
msg = SlackMessage({"text": "({}) ETL thread for {}".format(socket.gethostname(), this_day)})
msg.send()

def run(process, dataset=None, notify=False, emoji=None):
  try:
    p = etl.Process(process)
    p.extract()
    p.transform()
    if notify:
      msg.comment("Update successful: *{}*".format(process))
      msg.react(emoji)
  except Exception as e:
    pass
    # msg.comment("Error: *{}*\n > `{}`".format(process, e))

# # daily run; these do not use schedule
run('dlba', dataset=None, notify=False, emoji='bank')
run('assessor', dataset=None, notify=False, emoji='ledger')
run('medical_marijuana', dataset=None, notify=False, emoji='herb')
run('cad', dataset=None, notify=False, emoji='ambulance')
run('rms', dataset=None, notify=False, emoji='police_car')
run('bseed', dataset=None, notify=False, emoji='nut_and_bolt')
run('ocp', dataset=None, notify=False, emoji='money_with_wings')
run('blight_violations', dataset=None, notify=False, emoji='warning')

## Scheduling datasets
# Angel's Night fire data
# schedule.every(5).minutes.do(run, process='angels_night')

# # infinite loop
# while True:
#   # if it's the next day
#   if arrow.now().format('dddd, MMMM Do') != this_day:
#     # reset this_day to today
#     this_day = arrow.now().format('dddd, MMMM Do')
#     # create and send a new SlackMsg
#     msg = SlackMessage({"text": "ETL thread for {}".format(this_day)})
#     msg.send()

#   # run whatever is on deck
#   schedule.run_pending()

#   # wait a minute before checking again
#   time.sleep(1)
