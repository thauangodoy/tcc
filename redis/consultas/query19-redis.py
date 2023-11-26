import redis
import pandas as pd
from redis.commands.search.query import Query
import time
start_time = time.time()

# Initialize Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)
idx_lineitem = redis_client.ft('idx:lineitem')
idx_part = redis_client.ft('idx:part')

# Define function to query Redis and return a DataFrame
def query_redis(result):
    return pd.DataFrame([doc.__dict__ for doc in result.docs])

# Query lineitem and part data from Redis
# Adjust these queries as needed to fetch the relevant fields

result1 = idx_lineitem.search(Query("@l_quantity:[8 34] @l_shipmode:'AIR'|'AIR REG' @l_shipinstruct:'DELIVER IN PERSON'").paging(0, 1000000000))
result2 = idx_part.search(Query("@p_brand:'Brand#22'|'Brand#23'|'Brand#12' @p_container:'SM CASE'|'SM BOX'|'SM PACK'|'SM PKG'|'MED BAG'|'MED BOX'|'MED PKG'|'MED PACK'|'LG CASE'|'LG BOX'|'LG PACK'|'LG PKG' @p_size:[1 15]").paging(0, 1000000000))

lineitem_df = query_redis(result1)
part_df = query_redis(result2)

# Merge the DataFrames on part keys
merged_df = pd.merge(lineitem_df, part_df, left_on='partkey', right_on='partkey')

# Apply additional filters based on the SQL conditions
# Adjust these conditions as per your exact query logic
filtered_df = merged_df[
    (
        (merged_df['p_brand'] == 'Brand#22') &
        (merged_df['p_container'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
        (merged_df['l_quantity'].astype(int) >= 8) & 
        (merged_df['l_quantity'].astype(int) <= 18) & 
        (merged_df['p_size'].astype(int) <= 5) & 
        (merged_df['l_shipmode'].isin(['AIR', 'AIR REG'])) &
        (merged_df['l_shipinstruct'] == 'DELIVER IN PERSON')
    ) |
    (
        (merged_df['p_brand'] == 'Brand#23') &
        (merged_df['p_container'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
        (merged_df['l_quantity'].astype(int) >= 10) & 
        (merged_df['l_quantity'].astype(int) <= 20) & 
        (merged_df['p_size'].astype(int) <= 10) &
        (merged_df['l_shipmode'].isin(['AIR', 'AIR REG'])) &
        (merged_df['l_shipinstruct'] == 'DELIVER IN PERSON')
    ) |
    (
        (merged_df['p_brand'] == 'Brand#12') &
        (merged_df['p_container'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & 
        (merged_df['l_quantity'].astype(int) >= 24) & 
        (merged_df['l_quantity'].astype(int) <= 34) & 
        (merged_df['p_size'].astype(int) <= 15) &
        (merged_df['l_shipmode'].isin(['AIR', 'AIR REG'])) &
        (merged_df['l_shipinstruct'] == 'DELIVER IN PERSON')
    )
]

# Calculate revenue
filtered_df['revenue'] = filtered_df['l_extendedprice'].astype(float) * (1 - filtered_df['l_discount'].astype(float))
total_revenue = filtered_df['revenue'].astype(float).sum()

print(f"Total Revenue: {total_revenue}")
print("--- %s seconds ---" % (time.time() - start_time))

