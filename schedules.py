import time
import etl
import arrow
import socket

from etl.slack import SlackMessage

this_day = arrow.now().format('dddd, MMMM Do')
this_box = "production" if socket.gethostname() == "cod-etl-tools-fs2" else "development"
msg = SlackMessage({"text": "({}) ETL thread for {}".format(socket.gethostname(), this_day)})
msg.send()

def run(process, dataset=None, notify=False, emoji=None):
  try:
    p = etl.Process(process)
    p.update()
    if notify:
      msg.comment("Update successful: *{}*".format(process))
      msg.react(emoji)
  except Exception as e:
    pass
    msg.comment("Error: *{}*\n > ```{}```".format(process, e))

# daily run; these do not use schedule
run('bseed', dataset=None, notify=True, emoji='nut_and_bolt')
run('dlba', dataset=None, notify=True, emoji='bank')
run('assessor', dataset=None, notify=True, emoji='ledger')
run('ocp', dataset=None, notify=True, emoji='money_with_wings')
run('dah', dataset=None, notify=True, emoji='warning')
run('crio', dataset=None, notify=True, emoji='briefcase')
run('pubsafe', dataset=None, notify=True, emoji='police_car')
#run('graphql', dataset=None, notify=True, emoji='graphql')
run('scf', dataset=None, notify=True, emoji='telephone_receiver')
#run('dba', dataset=None, notify=True, emoji='dollar')

