import schedule
import time
import etl

def run(process):
  p = etl.Process(process)
  p.update()

schedule.every(1).minutes.do(run, process='angels_night')
# schedule.every.tuesday.do(run('medical_marijuana'))

while True:
    schedule.run_pending()
    time.sleep(1)