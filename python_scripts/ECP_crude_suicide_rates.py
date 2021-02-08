import pandas as pd

df_crudeSuicideRates = pd.read_csv("/home/alimesut/DEV/datasets/world_health_data_sets/crudeSuicideRates.csv")

print("Column headings:")
print(df_crudeSuicideRates.columns)

df_db_crudeSuicideRates = pd.concat([df_crudeSuicideRates.iloc[:, 0:2], df_crudeSuicideRates.iloc[:, 3:5]], axis=1)
df_db_crudeSuicideRates.columns = ['location', 'period', 'gender', 'first_tooltip']

print(df_db_crudeSuicideRates.columns)

# ---------------------------------------------------------------------------------------------------------------------#

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


engine = create_engine('postgresql://admin:secret@localhost:54332/postgres', )
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
# tablo yada kolon isimlendirmesinde tırnak işareti eklememesi için aşağıdaki 2 satır çalıştırılır.
engine.dialect.identifier_preparer.initial_quote = ''
engine.dialect.identifier_preparer.final_quote = ''

class CrudeSuicideRates(Base):
    """Data model crude_suicide_rates."""
    __tablename__ = 'crude_suicide_rates'
    __table_args__ = {'schema': 'public'}

    # SQLALchemy ORM yapısı içinde PK tanımlı bir kolon olmadan çalışmaz bu nedenle unique olması için uuid tanımlandı.
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    location = Column('location', String(100), nullable=False, quote=False)
    period = Column('period', Integer, nullable=False)
    gender = Column('gender', String(100), nullable=False)
    first_tooltip = Column('first_tooltip', Integer, nullable=False)


# CREATE TABLE
if not engine.dialect.has_table(engine, CrudeSuicideRates.__tablename__):
    Base.metadata.create_all(engine)


df_db_crudeSuicideRates.to_sql(CrudeSuicideRates.__tablename__, engine, index=False, if_exists="append", schema="public", chunksize=1000)
