import os, requests, schedule, time, queue, threading, logging
from datetime import datetime
moex_host = os.environ.get('MOEX_HOST')
news_host = os.environ.get('NEWS_HOST')

def kick_moex():
    logging.info("Kicking moex.....")
    current_date = str(datetime.now().date().isoformat())
    requests.get(moex_host + "/" + current_date)

def kick_news():
    logging.info("Kicking news.......")
    requests.get(news_host + "/")


def worker_main():
    while True:
        job_func = jobqueue.get()
        job_func()
        jobqueue.task_done()


jobqueue = queue.Queue()

schedule.every().day.at("00:00").do(jobqueue.put, kick_moex)
schedule.every().hour.do(jobqueue.put, kick_news)

worker_thread = threading.Thread(target=worker_main)
worker_thread.start()

while True:
    schedule.run_pending()
    time.sleep(1)
