import schedule
import time
import etl

def angels_night_update():
  an = etl.Process('angels_night')
  an.update()

schedule.every(5).minutes.do(angels_night_update)

while True:
    schedule.run_pending()
    time.sleep(1)