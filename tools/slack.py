import os, json, requests

# class ETLAlert(object):
class SlackMessage(object):
  def __init__(self, data):
    self.data = data

  def send(self):
    slack_data = self.data
    response = requests.post(os.environ['SLACK_WEBHOOK'], data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
      raise ValueError(
        'Request to Slack returned an error %s, the response is:\n%s'
        % (response.status_code, response.text)
        )

if __name__ == "__main__":
  msg = SlackMessage({'text': "Testing this module"})
  msg.send()
