import redis
import pandas as pd
from redis.commands.search.query import Query
import time
start_time = time.time()

# Initialize Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)
idx_lineitem = redis_client.ft('idx:lineitem')
idx_order = redis_client.ft('idx:order')
idx_customer = redis_client.ft('idx:customer')
idx_nation = redis_client.ft('idx:nation')

# Define function to query Redis and return a DataFrame
def query_redis(result):
    return pd.DataFrame([doc.__dict__ for doc in result.docs])


result1 = idx_lineitem.search(Query("@l_returnflag:'R'").paging(0, 10000000))
result2 = idx_order.search(Query("@o_orderdate:[757389600.0 765169200.0]").paging(0, 10000000))
result5 = idx_nation.search(Query("*").paging(0, 10000000)) 
result3 = idx_customer.search(Query("*").paging(0, 10000000))

lineitem_df = query_redis(result1)
order_df = query_redis(result2)
customer_df = query_redis(result3)
nation_df = query_redis(result5)


order_df = order_df.drop('payload', axis=1)
customer_df = customer_df.drop('payload', axis=1)
lineitem_df = lineitem_df.drop('payload', axis=1)
nation_df = nation_df.drop('payload', axis=1)

order_df = order_df.drop('id', axis=1)
customer_df = customer_df.drop('id', axis=1)
lineitem_df = lineitem_df.drop('id', axis=1)
nation_df = nation_df.drop('id', axis=1)

df_joined = customer_df.merge(order_df, left_on='c_custkey', right_on='o_custkey')
df_joined = df_joined.merge(lineitem_df, left_on='o_orderkey', right_on='l_orderkey')
df_joined = df_joined.merge(nation_df, left_on='c_nationkey', right_on='n_nationkey')

# Calculate revenue
df_joined['revenue'] = df_joined['l_extendedprice'].astype(float) * (1 - df_joined['l_discount'].astype(float))

# Group by the specified fields and sort by revenue
grouped = df_joined.groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment'])
result = grouped['revenue'].sum().sort_values(ascending=False).head(20)

# Print or return the final result
print(result)
print("--- %s seconds ---" % (time.time() - start_time))