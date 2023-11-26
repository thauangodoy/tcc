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

# Define function to query Redis and return a DataFrame
def query_redis(result):
    return pd.DataFrame([doc.__dict__ for doc in result.docs])

result1 = idx_lineitem.search(Query("@l_shipdate:[796446000.0 +inf]").paging(0, 10000000))
result2 = idx_order.search(Query("@o_orderdate:[-inf 796446000.0]").paging(0, 10000000))
result3 = idx_customer.search(Query("@c_mktsegment:{FURNITURE}").paging(0, 10000000)) 

lineitem_df = query_redis(result1)
order_df = query_redis(result2)
customer_df = query_redis(result3)

orders_customers = pd.merge(order_df, customer_df, left_on='o_custkey', right_on='c_custkey')
combined = pd.merge(orders_customers, lineitem_df, left_on='o_orderkey', right_on='l_orderkey')

print(combined)

combined['revenue'] = combined['l_extendedprice'].astype(float) * (1 - combined['l_discount'].astype(float))
result = combined.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg({'revenue': 'sum'}).reset_index()
result = result.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True])
result = result.head(10)

# Display the result
print(result)
print("--- %s seconds ---" % (time.time() - start_time))