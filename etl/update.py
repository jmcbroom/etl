from etl.process import Process
from etl.slack import SlackMessage

class Update(object):
  def __init__(self, process='angels_night'):
    self.proc = Process(process)
  
  def perform_update(self):
    if self.proc.notify:
      msg = SlackMessage({"text": "Starting update: {}".format(self.proc.name)})
      msg.send()
    self.proc.extract()
    self.proc.transform()
    self.proc.load()
    if self.proc.notify:
      for d in self.proc.destinations:
        if d == 'arcgis-online':
          msg.react('briefcase')
        elif d == 'socrata':
          msg.react('umbrella')

