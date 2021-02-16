import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
# from python_scripts import parameters as prm
import sys

# Data extract from CSV file to DataFrame #

df_weather = pd.read_csv('/opt/airflow/datasets/weather/%s.csv' % sys.argv[1])

df_db_weather = df_weather.iloc[:, 0:37]
df_db_weather.columns = ['datetime', 'Vancouver', 'Portland', 'San_Francisco', 'Seattle', 'Los_Angeles',
                         'San_Diego', 'Las_Vegas', 'Phoenix', 'Albuquerque', 'Denver', 'San_Antonio', 'Dallas',
                         'Houston', 'Kansas_City', 'Minneapolis', 'Saint_Louis', 'Chicago', 'Nashville',
                         'Indianapolis', 'Atlanta', 'Detroit', 'Jacksonville', 'Charlotte', 'Miami',
                         'Pittsburgh', 'Toronto', 'Philadelphia', 'New_York', 'Montreal', 'Boston',
                         'Beersheba', 'Tel_Aviv_District', 'Eilat', 'Haifa', 'Nahariyya', 'Jerusalem']

df_weather_unpivot = df_db_weather.melt(id_vars='datetime', var_name='Cities', value_name=sys.argv[1])

print(df_weather_unpivot)

# -------------------------------------------------------------------------------------------------------------------- #

engine = create_engine('postgresql://admin:secret@postgre_db:5432/postgres', )
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
# tablo yada kolon isimlendirmesinde tırnak işareti eklememesi için aşağıdaki 2 satır çalıştırılır.
engine.dialect.identifier_preparer.initial_quote = ''
engine.dialect.identifier_preparer.final_quote = ''


class weather(Base):
    """Data model weather_attributes."""
    __tablename__ = 'weather_%s' % sys.argv[1]
    __table_args__ = {'schema': 'ext'}

    # SQLALchemy ORM yapısı içinde PK tanımlı bir kolon olmadan çalışmaz bu nedenle unique olması için uuid tanımlandı.
    sk = Column('%s_sk' % sys.argv[1], Integer, primary_key=True, autoincrement=True)
    datetime = Column('datetime', TIMESTAMP, nullable=True)
    cities = Column('cities', String(200), nullable=True)
    if sys.argv[1] == 'description':
        weather_attribute = Column('%s' % sys.argv[1], String(200), nullable=True)
    else:
        weather_attribute = Column('%s' % sys.argv[1], Numeric, nullable=True)


# CREATE TABLE
if not engine.dialect.has_table(engine, weather.__tablename__):
    Base.metadata.create_all(engine)

df_weather_unpivot.to_sql(weather.__tablename__, engine, index=False,
                          if_exists="append", schema="ext", chunksize=1000)
