import os, yaml

os.environ['DATA_DIR'] = '/home/jimmy/Work/etl/datasets'

class Dataset(object):
  def __init__(self, directory="{}/dah/blight_notices".format(os.environ['DATA_DIR'])):
    with open("{}/config.yml".format(directory), 'r') as f:
      self.conf = yaml.load(f)
    print(self.conf)