## Script execution /home/user_XPTO/GITHUB/bin/cronjob_mixpanel_daily_datapipeline.sh
PATH=~/miniconda3/bin:$PATH
export PATH
python3 mixpanel_daily_datapipeline.py local >../log/cron_job.log