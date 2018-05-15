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
    if socket.gethostname() == "cod-etl-tools-fs2":
      p.load()
      if notify:
        msg.comment("Update successful: *{}*".format(process))
        msg.react(emoji)
  except Exception as e:
    pass
    msg.comment("Error: *{}*\n > `{}`".format(process, e))

# daily run; these do not use schedule
run('dlba', dataset=None, notify=True, emoji='bank')
run('assessor', dataset=None, notify=True, emoji='ledger')
run('medical_marijuana', dataset=None, notify=True, emoji='herb')
run('cad', dataset=None, notify=True, emoji='ambulance')
run('rms', dataset=None, notify=True, emoji='police_car')
run('bseed', dataset=None, notify=True, emoji='nut_and_bolt')
run('ocp', dataset=None, notify=True, emoji='money_with_wings')
run('blight_violations', dataset=None, notify=True, emoji='warning')
run('crio', dataset=None, notify=True, emoji='briefcase')
run('dba', dataset=None, notify=True, emoji='dollar')
run('project_greenlight', dataset=None, notify=True, emoji='traffic_light')

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
