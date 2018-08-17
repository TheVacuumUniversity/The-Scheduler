from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from config import Config

class Database:

    Base = declarative_base()

    @classmethod
    def initialize(cls):
        cls.engine = create_engine(Config.DATABASE_URI)
        cls.session_factory = sessionmaker(bind=Database.engine)
        cls.Session = scoped_session(Database.session_factory)

    @classmethod
    def get_session(cls):
        return cls.Session()
