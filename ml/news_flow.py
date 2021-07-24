import pandas as pd
import datetime
from ml_setup import session, AllNews, NewsFlow
from sqlalchemy import cast, Date
import warnings, logging


def warn(*args, **kwargs):
    pass


warnings.warn = warn

def update_flow():
	logging.info("Updating flow...")
	current_time = datetime.datetime.now().date()
	stmt = session.query(AllNews).filter((AllNews.datetime.cast(Date) == current_time) & (AllNews.rubric.any("Экономика"))).statement
	df = pd.read_sql(stmt, session.bind)
	slava_marlow = NewsFlow()
	slava_marlow.write_last_news(df)


if __name__ == '__main__':
	update_flow()
