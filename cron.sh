#!/bin/bash

# Setting up cron jobs

# Define log directory
LOG_DIR="/root/market-data-pipeline/logs"
mkdir -p $LOG_DIR

# Remove existing cron jobs related to market-data-pipeline
crontab -l | grep -v 'market-data-pipeline' | crontab -

# Add cron job for daily data collection at 10:00 PM UTC
(crontab -l ; echo "0 22 * * * /usr/bin/python3 /root/market-data-pipeline/collect_and_send_to_db_15yrs_1d.py >> $LOG_DIR/daily_data.log 2>&1") | crontab -

# Add cron job for 1-minute data collection every day at 9:00 AM UTC
(crontab -l ; echo "0 9 * * * /usr/bin/python3 /root/market-data-pipeline/collect_and_send_to_db_24hr_1min.py >> $LOG_DIR/minute_data.log 2>&1") | crontab -

# Add cron job for cleaning databases every hour
(crontab -l ; echo "0 * * * * /usr/bin/python3 /root/market-data-pipeline/clean_duplicates_both_db.py >> $LOG_DIR/cleaning.log 2>&1") | crontab -

echo "Cron jobs successfully set up!"

