from slackclient import SlackClient

from os import environ as env

slack_token = env["SLACK_API_TOKEN"]
sc = SlackClient(slack_token)

class SlackMessage(object):
  def __init__(self, data):
    self.data = data
  
  def send(self):
    m = sc.api_call(
      "chat.postMessage",
      channel="#z_etl",
      text=self.data['text']
    )
    self.channel = m['channel']
    self.ts = m['ts']

  def react(self, emoji):
    r = sc.api_call(
      "reactions.add",
      channel = self.channel,
      name = emoji,
      timestamp = self.ts
    )