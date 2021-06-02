import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data.db")

query = """ select * from users;"""
lib = pd.read_sql(query,engine)
print(lib)