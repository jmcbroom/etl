import time
import etl
import arrow

from etl.slack import SlackMessage

this_day = arrow.now().format('dddd, MMMM Do')
msg = SlackMessage({"text": "ETL hourly updates for {}".format(this_day)})

def run(process, dataset=None, notify=False, emoji=None):
  try:
    p = etl.Process(process)
    p.update(dataset)
    if notify:
      msg.comment("Update successful: *{}*".format(process))
      msg.react(emoji)
  except Exception as e:
    msg.send()
    msg.comment("Error: *{}*\n > `{}`".format(process, e))

# # daily run; these do not use schedule
run('crimescape', dataset=None, notify=False, emoji='rotating_light')
run('angels_night', dataset=None, notify=False, emoji='fire')
