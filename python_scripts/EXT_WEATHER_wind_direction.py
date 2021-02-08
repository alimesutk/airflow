import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Data extract from CSV file to DataFrame #

df_weather_wind_direction = pd.read_csv("../datasets/weather/wind_direction.csv")

df_db_weather_wind_direction = df_weather_wind_direction.iloc[:, 0:37]
df_db_weather_wind_direction.columns = ['datetime', 'Vancouver', 'Portland', 'San_Francisco', 'Seattle', 'Los_Angeles',
                                        'San_Diego', 'Las_Vegas', 'Phoenix', 'Albuquerque', 'Denver', 'San_Antonio',
                                        'Dallas',
                                        'Houston', 'Kansas_City', 'Minneapolis', 'Saint_Louis', 'Chicago', 'Nashville',
                                        'Indianapolis', 'Atlanta', 'Detroit', 'Jacksonville', 'Charlotte', 'Miami',
                                        'Pittsburgh', 'Toronto', 'Philadelphia', 'New_York', 'Montreal', 'Boston',
                                        'Beersheba', 'Tel_Aviv_District', 'Eilat', 'Haifa', 'Nahariyya', 'Jerusalem']

df_weather_wind_direction_unpivot = df_db_weather_wind_direction.melt(id_vars='datetime', var_name='Cities',
                                                                      value_name='wind_direction')

print(df_weather_wind_direction_unpivot)

# -------------------------------------------------------------------------------------------------------------------- #

engine = create_engine('postgresql://admin:secret@localhost:54332/postgres', )
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
# tablo yada kolon isimlendirmesinde tırnak işareti eklememesi için aşağıdaki 2 satır çalıştırılır.
engine.dialect.identifier_preparer.initial_quote = ''
engine.dialect.identifier_preparer.final_quote = ''


class weather_wind_direction(Base):
    """Data model weather_wind_direction."""
    __tablename__ = 'weather_wind_direction'
    __table_args__ = {'schema': 'ext'}

    # SQLALchemy ORM yapısı içinde PK tanımlı bir kolon olmadan çalışmaz bu nedenle unique olması için uuid tanımlandı.
    wind_direction_sk = Column('wind_direction_sk', Integer, primary_key=True, autoincrement=True)
    datetime = Column('datetime', TIMESTAMP, nullable=True)
    cities = Column('cities', String(200), nullable=True)
    wind_direction = Column('wind_direction', Numeric, nullable=True)


# CREATE TABLE
if not engine.dialect.has_table(engine, weather_wind_direction.__tablename__):
    Base.metadata.create_all(engine)

df_weather_wind_direction_unpivot.to_sql(weather_wind_direction.__tablename__, engine, index=False,
                                         if_exists="append", schema="ext", chunksize=10000)
