import schedule
import time

def job():
    execfile('data_ingestion.py')
    print('Data updated')
    execfile('dataproc.py')
    print("local clusters updated")
    execfile('pyspark_job.py')
    print("Pyspark successfully ran")

schedule.every().monday.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
