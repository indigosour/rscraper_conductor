from sqlalchemy import create_engine,Column,Integer,String,Boolean,DateTime,DECIMAL,text
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm
from sqlalchemy.exc import IntegrityError, DataError, SQLAlchemyError
import logging,datetime
from datetime import timedelta, datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from common import *

db_name = 'reddit_scraper'
database_url = f"mysql+pymysql://{get_az_secret('DB-CRED')['username']}:{get_az_secret('DB-CRED')['password']}@{get_az_secret('DB-CRED')['url']}:3306/{db_name}"
Base = sqlalchemy.orm.declarative_base()

engine = create_engine(database_url)

def create_sqlalchemy_session():
    try:
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception as e:
        print("Error creating sql session",e)
        logging.error("Error creating sql session")
    return session


class Post(Base):
    __tablename__ = 'posts'

    post_id = Column(String(8), primary_key=True)
    title = Column(String(500))
    author = Column(String(255))
    subreddit = Column(String(255))
    score = Column(Integer)
    upvote_ratio = Column(DECIMAL(precision=5, scale=4))
    num_comments = Column(Integer)
    created_utc = Column(DateTime)
    last_updated = Column(DateTime)
    is_downloaded = Column(Boolean)
    permalink = Column(String(255))
    is_original_content = Column(Boolean)
    over_18 = Column(Boolean)

    def __repr__(self):
        return f"<Post(id='{self.post_id}', title='{self.title}', subreddit='{self.subreddit}')>"


class Inventory(Base):
    __tablename__ = 'inventory'

    post_id = Column(String(length=8), primary_key=True)
    last_updated = Column(DateTime)
    watched = Column(Boolean)
    stored = Column(Boolean)
    tube_id = Column(String(length=255))
    videohash = Column(String(length=66),unique=True)

    def __repr__(self):
        return f"<Inventory(id='{self.post_id}')>"


def get_dl_list_period(period):
    session = create_sqlalchemy_session()
    logging.info(f"get_dl_list_period: Getting download list for the period {period} ")

    # Determine the start and end dates based on the period given
    if period == "day":
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    elif period == "week":
        start_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    elif period == "month":
        start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    elif period == "year":
        start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    else:
        print("Invalid period")

    logging.debug(f"get_dl_list_period: Period calculated \nStart Date: {start_date} and End Date: {end_date}")

    try:
        with engine.connect() as connection:
            session = sqlalchemy.orm.Session(bind=connection)
            query = session.query(Post.title, Post.post_id, Post.permalink, Post.author, Post.permalink,
                                    Post.score, Post.upvote_ratio, Post.num_comments, Post.created_utc, Post.subreddit).filter(
                Post.created_utc.between(start_date, end_date), Post.score > 500).all()
            logging.debug(f"get_dl_list_period: Query - {query}")
            dl_list = [{"title": row.title, 
                        "subreddit": row.subreddit,
                        "id": row.post_id, 
                        "author": row.author,
                        "permalink": row.permalink, 
                        "score": row.score,
                        "upvote_ratio": float(row.upvote_ratio),
                        "num_comments": row.num_comments,
                        "created_utc": str(row.created_utc),
                        } for row in query]
            return dl_list
        
    except Exception as e:
        print("Error reading data from table", e)
        logging.error("get_dl_list_period: Error reading data from table", e)
    finally:
        session.close()