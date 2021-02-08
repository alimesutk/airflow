import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# Data extract from CSV file to DataFrame #

df_ecommerce = pd.read_csv("/home/alimesut/DEV/datasets/ecommerce.csv")

print("Column headings:")
print(df_ecommerce.columns)

df_db_ecommerce = df_ecommerce.iloc[:, 0:9]
df_db_ecommerce.columns = ['Invoice_No', 'Stock_Code', 'Description', 'Quantity',
                           'Invoice_Date', 'Unit_Price', 'Customer_Id', 'Country']

print(df_db_ecommerce.columns)

# -------------------------------------------------------------------------------------------------------------------- #

engine = create_engine('postgresql://admin:secret@localhost:54332/postgres', )
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
# tablo yada kolon isimlendirmesinde tırnak işareti eklememesi için aşağıdaki 2 satır çalıştırılır.
engine.dialect.identifier_preparer.initial_quote = ''
engine.dialect.identifier_preparer.final_quote = ''


class ecommerce(Base):
    """Data model ecommerce."""
    __tablename__ = 'ecommerce'
    __table_args__ = {'schema': 'public'}

    # SQLALchemy ORM yapısı içinde PK tanımlı bir kolon olmadan çalışmaz bu nedenle unique olması için uuid tanımlandı.
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    InvoiceNo = Column('Invoice_No', String(200), nullable=True)
    StockCode = Column('Stock_Code', String(200), nullable=True)
    Description = Column('Description', String(4000), nullable=True)
    Quantity = Column('Quantity', Integer, nullable=True)
    InvoiceDate = Column('Invoice_Date', TIMESTAMP, nullable=True)
    UnitPrice = Column('Unit_Price', Numeric, nullable=True)
    CustomerID = Column('Customer_Id', Integer, nullable=True)
    Country = Column('Country', String(200), nullable=True)


# CREATE TABLE
if not engine.dialect.has_table(engine, ecommerce.__tablename__):
    Base.metadata.create_all(engine)

df_db_ecommerce.to_sql(ecommerce.__tablename__, engine, index=False, if_exists="append", schema="public", chunksize=1000)
