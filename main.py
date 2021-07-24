import warnings
import os, requests, schedule, time, threading, logging, sys
from datetime import datetime
from plots.fin_plot import upd_secs_plots
moex_host = os.environ.get('MOEX_HOST')
news_host = os.environ.get('NEWS_HOST')


def warn(*args, **kwargs):
    pass


warnings.warn = warn

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def kick_moex():
    logging.info("Kicking moex.....")
    weekday = datetime.today().weekday()
    if weekday == 4:
        today = datetime.now().date()
        #yesterday = today - datetime.timedelta(days=1)
        requests.get(moex_host + "/" + str(today))
        upd_secs_plots()


def kick_news():
    logging.info("Kicking news.......")
    requests.get(news_host + "/")


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


schedule.every().day.at("21:00").do(run_threaded, kick_moex)
schedule.every(30).minutes.do(run_threaded, kick_news)

while True:
    schedule.run_pending()
    time.sleep(1)
