from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


engine = create_engine('mysql+mysqlconnector://root:Netsolpk1@localhost/may_data_dump')
Session = sessionmaker(bind=engine)

Base = declarative_base()
