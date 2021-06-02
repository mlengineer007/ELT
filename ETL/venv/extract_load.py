import requests
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///data.db',echo = False)


api_url = "https://api.yelp.com/v3/businesses/search"
api_key = "7FYZLDCila_tEWjX_2aXFar9Qs-QlpnQx_8s0umFGsYNOTg3r6xn6JhUtc0G2ogI8WlGxd3sMZvTTRKzmBloadnb9Y0XtFzK0_85eQoF6b5bDTHXM3QSa9SKfH22YHYx"
params = {"term": "bookstore",
          "location" : "NJ",
          "radius":40000,
          "offset":50}
headers = {"Authorization": "Bearer %s" %api_key}
response = requests.get(api_url, params=params,headers=headers)

data = response.json()

print(data)


name = []
rating = []
review_count = []
lat = []
long = []
for i in data["businesses"]:

     name.append(i["name"])
     rating.append(i["rating"])
     review_count.append(i["review_count"])
     lat.append(i["coordinates"]["latitude"])
     long.append(i["coordinates"]["longitude"])

k = pd.DataFrame({"name":name,"rating":rating,"review_count":review_count,"lat":lat,"long":long})
top = k.sort_values(["rating","review_count"],ascending=False,ignore_index=True)
top_10 = top.head(10)

with engine.begin() as connection:
    k.to_sql('users', con=connection, if_exists='append')
