from sqlalchemy import create_engine,Column,Integer,String,Boolean,DateTime,DECIMAL,text
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm
from sqlalchemy.exc import IntegrityError, DataError, SQLAlchemyError
import logging,datetime
from datetime import timedelta, datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from common import *
from reddit import *

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


def create_table():
    Base.metadata.create_all(engine)

def drop_table():
    Base.metadata.drop_all(engine)


def store_reddit_posts(sub):
    session = create_sqlalchemy_session()
    logging.info(f"store_reddit_posts: Storing reddit posts...")
    sub_entrycount = 0
    
    for period in ["day","week","month","year","all"]:
        period_entrycount = 0
        posts = get_reddit_posts(sub,period)
        for i in posts:
            if i.author != None and i.is_video == True:
                post = Post(
                    post_id=i.id,
                    title=cleanString(i.title)[:500],
                    author=i.author.name,
                    subreddit=i.subreddit,
                    score=i.score,
                    upvote_ratio=i.upvote_ratio,
                    num_comments=i.num_comments,
                    created_utc=datetime.fromtimestamp(int(i.created_utc)),
                    is_original_content=i.is_original_content,
                    over_18=i.over_18,
                    permalink="https://reddit.com" + i.permalink,
                    last_updated=datetime.now()
                )

                try:
                    session.execute(
                        text("""
                            INSERT INTO posts (post_id, title, author, subreddit, score, upvote_ratio, num_comments, created_utc, is_original_content, over_18, permalink, last_updated)
                            VALUES (:post_id, :title, :author, :subreddit, :score, :upvote_ratio, :num_comments, :created_utc, :is_original_content, :over_18, :permalink, :last_updated)
                        """),
                        {
                            "post_id": post.post_id,
                            "title": post.title,
                            "author": post.author,
                            "subreddit": post.subreddit,
                            "score": post.score,
                            "upvote_ratio": post.upvote_ratio,
                            "num_comments": post.num_comments,
                            "created_utc": post.created_utc,
                            "is_original_content": post.is_original_content,
                            "over_18": post.over_18,
                            "permalink": post.permalink,
                            "last_updated": post.last_updated
                        }
                    )
                    session.commit()
                    period_entrycount += 1
                    sub_entrycount += 1
                except (IntegrityError, DataError):
                    session.rollback()
                    continue

        print(f"Successfully added {period_entrycount} entries for {sub} to the database for the period {period}")
        logging.info(f"Successfully added {period_entrycount} entries for {sub} to the database for the period {period}")
        
    session.close()
    logging.info(f"store_reddit_posts: Successfully added {sub_entrycount} entries for {sub} to the database")
    return print(f"Successfully added {sub_entrycount} entries for {sub} to the database")


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


# def update_posts(post_id, score, num_comments):
#     session = create_sqlalchemy_session()

#     for submission in posts:
#         if submission.author != None and submission.is_video == True:
#             post = Post(
#                 post_id=submission.id,
#                 title=cleanString(submission.title)[:500],
#                 author=submission.author.name,
#                 subreddit=submission.subreddit,
#                 score=submission.score,
#                 upvote_ratio=submission.upvote_ratio,
#                 num_comments=submission.num_comments,
#                 created_utc=datetime.fromtimestamp(int(submission.created_utc)),
#                 is_original_content=submission.is_original_content,
#                 over_18=submission.over_18,
#                 permalink="https://reddit.com" + submission.permalink,
#                 last_updated=datetime.now()
#             )

#     try:
#         session.query(Post).filter_by(post_id=post_id).update(
#             {
#                 'score': score,
#                 'num_comments': num_comments,
#                 'upvote_ratio': upvote_ratio,
#                 'last_updated': datetime.now()
#             })

#         session.commit()
#         logging.info(f"update_posts: Successfully updated post {post_id}")
#     except SQLAlchemyError as e:
#         session.rollback()
#         logging.error(f"update_posts: Failed to update post {post_id}. Error message: {e}")
#     finally:
#         session.close()


def insert_inventory(hash, post_id, vid_uuid):
    session = create_sqlalchemy_session()
    try:
        new_item = Inventory(
            videohash=hash,
            post_id=post_id,
            stored=True,
            watched=False,
            tube_id=vid_uuid,
            last_updated=datetime.now()
        )
        session.add(new_item)
        session.commit()
        logging.info(f"insert_inventory: Successfully inserted new inventory item {post_id}")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"insert_inventory: Failed to insert new inventory item {post_id}. Error message: {e}")
    finally:
        session.close()


def hash_inventory_check(hash):
    session = create_sqlalchemy_session()
    exists = False
    with engine.connect() as connection:
        try:
            session = sqlalchemy.orm.Session(bind=connection)
            query = session.query(Inventory.videohash).filter(Inventory.videohash.like(hash)).all()
            logging.debug(f"hash_inventory_check: Query - {query}")
        except Exception as e:
            session.rollback()
            logging.error(f"hash_inventory_check: Failed to check video hash in inventory table. {query}")
        finally:
            session.close()
    if len(query) > 0:
        exists = True
    return exists


def id_inventory_check(post_id):
    session = create_sqlalchemy_session()
    with engine.connect() as connection:
        try:
            session = sqlalchemy.orm.Session(bind=connection)
            query = session.query(Inventory.post_id,Inventory.tube_id,Inventory.watched).filter(Inventory.post_id.like(post_id)).all()
            logging.debug(f"id_inventory_check: Query - {query}")
        except Exception as e:
            session.rollback()
            logging.error(f"id_inventory_check: Failed to check {post_id} in inventory table. Query: {query} Error message: {e}")
        finally:
            session.close()
    return query