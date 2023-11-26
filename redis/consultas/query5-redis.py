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
idx_region = redis_client.ft('idx:region')
idx_nation = redis_client.ft('idx:nation')
idx_supplier = redis_client.ft('idx:supplier')

# Define function to query Redis and return a DataFrame
def query_redis(result):
    return pd.DataFrame([doc.__dict__ for doc in result.docs])

result4 = idx_region.search(Query("@r_name:'MIDDLE EAST'").paging(0, 10000000))
result2 = idx_order.search(Query("@o_orderdate:[757303200.0 788925600.0]").paging(0, 10000000))

region_df = query_redis(result4)
order_df = query_redis(result2)

mini = order_df['o_orderkey'].astype(int).min()
maxi = order_df['o_orderkey'].astype(int).max()
result1 = idx_lineitem.search(Query("@l_orderkey:["+str(mini)+" "+str(maxi)+"]").paging(0, 10000000))

lineitem_df = query_redis(result1)

result6 = idx_supplier.search(Query("*").paging(0, 10000000))
result5 = idx_nation.search(Query("*").paging(0, 10000000)) 
result3 = idx_customer.search(Query("*").paging(0, 10000000))

customer_df = query_redis(result3)
nation_df = query_redis(result5)
supplier_df = query_redis(result6)

order_df = order_df.drop('payload', axis=1)
customer_df = customer_df.drop('payload', axis=1)
lineitem_df = lineitem_df.drop('payload', axis=1)
supplier_df = supplier_df.drop('payload', axis=1)
nation_df = nation_df.drop('payload', axis=1)
region_df = region_df.drop('payload', axis=1)

order_df = order_df.drop('id', axis=1)
customer_df = customer_df.drop('id', axis=1)
lineitem_df = lineitem_df.drop('id', axis=1)
supplier_df = supplier_df.drop('id', axis=1)
nation_df = nation_df.drop('id', axis=1)
region_df = region_df.drop('id', axis=1)

print(supplier_df)
print(lineitem_df)

df_joined = order_df.merge(customer_df, left_on='o_custkey', right_on='c_custkey')
df_joined = df_joined.merge(lineitem_df, left_on='o_orderkey', right_on='l_orderkey')
df_joined = df_joined.merge(supplier_df, left_on='l_suppkey', right_on='s_suppkey')
df_joined = df_joined.merge(nation_df, left_on='s_nationkey', right_on='n_nationkey')
df_joined = df_joined.merge(region_df, left_on='n_regionkey', right_on='r_regionkey')

# Calculate revenue
df_joined['revenue'] = df_joined['l_extendedprice'].astype(float) * (1 - df_joined['l_discount'].astype(float))

# Group by n_name and sort by revenue
result = df_joined.groupby('n_name')['revenue'].sum().sort_values(ascending=False)

# Print or return the final result
print(result)
print("--- %s seconds ---" % (time.time() - start_time))