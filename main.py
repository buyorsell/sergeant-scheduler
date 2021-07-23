import os, requests, schedule, time, queue, threading, logging, sys
from datetime import datetime
from plots.fin_plot import upd_secs_plots
moex_host = os.environ.get('MOEX_HOST')
news_host = os.environ.get('NEWS_HOST')

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def kick_moex():
    logging.info("Kicking moex.....")
    weekday = datetime.today().weekday()
    if weekday == 5:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        requests.get(moex_host + "/" + str(today))
        upd_secs_plots()

def kick_news():
    logging.info("Kicking news.......")
    requests.get(news_host + "/")


def worker_main():
    while True:
        job_func = jobqueue.get()
        job_func()
        jobqueue.task_done()


jobqueue = queue.Queue()

schedule.every().day.at("21:00").do(jobqueue.put, kick_moex)
schedule.every().hour.do(jobqueue.put, kick_news)

worker_thread = threading.Thread(target=worker_main)
worker_thread.start()

while True:
    schedule.run_pending()
    time.sleep(1)
