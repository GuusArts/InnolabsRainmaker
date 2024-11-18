from datetime import datetime
import pytz
import time
import schedule

def update_dashboard():
    ""
    print(f"dashboard update at {datetime.now().strftime('%y-%m-%d %H:%M:%S')}")
    
#setup amsterdam timezone
amsterdam_timezone = pytz.timezone('Europe/Amsterdam')

def job_wrapper():
    
    current_time = datetime.now(amsterdam_timezone)
    print(F"Running schedule job at {current_time.strftime('%y-%m-%d %H:%M:%S')} (Amsterdam time)")
    update_dashboard()
    
#scedule tasks @

schedule.every().day.at("08:00").do(job_wrapper)
schedule.every().day.at("19:00").do(job_wrapper)

print("Task scheduler initialized. Waiting for next scheduled time...")

while True:
    schedule.run_pending()
    time.sleep(1)