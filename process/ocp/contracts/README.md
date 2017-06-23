# SFTP

Scripts to get data from a remote SFTP server, clean and load it into a central database, and then publish to Socrata.

This example works specifically with the City of Detroit Contracts dataset.

## Schedule this as a cron job

1. Clone this repo to wherever you want to run the job from. In our case, this is a remote Linux server.
2. Edit `daily-update-socrata.sh` and replace `/path/to/` with your full directories.
3. Edit `socrata_replace.py` with the full path to your yml, around line 16:
```
with open('/path/to/etl/sftp/config.yml', 'r') as f:
  config = yaml.load(f)
```
4. Add a new cron task: `crontab -e`
5. Configure the job to run at 6am Mon-Fri and log errors to a local file: 
```
0 6 * * 1-5 /bin/bash /path/to/etl/sftp/daily-update-socrata.sh >> /path/to/cron-output.log 2>&1
```
6. List all cron jobs to confirms it's scheduled: `crontab -l`

If it's still buggy, try adding two path variables to the top of your cron file:
```
PATH=/sbin:/bin:/usr/sbin:/usr/bin:$HOME/bin
PTYHONPATH=/path/to/anaconda3/lib/python3.6/site-packages
```
