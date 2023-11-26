import redis
import pandas
from datetime import datetime

#REDIS CONNECTION SETUP#

r = redis.Redis(
    host='localhost',
    port='6379', 
    password='')

## REGION ##

def importRegions():

    region_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/region.tbl', sep='|', header=None)

    region_data.columns = ['_id', 'r_name', 'r_comment']

    dict_region_data = [x[1].to_dict() for x in region_data.iterrows()]

    for region in dict_region_data:

        r.hmset("region:"+str(region['_id']), {
            'r_regionkey': region['_id'],
            'r_name': region['r_name'],
            'r_comment': region['r_comment'],
        })

    print("FINISHED REGIONS")


## NATIONS ##

def importNations():

    nation_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/nation.tbl', sep='|', header=None)

    nation_data.columns = ['_id', 'n_name', 'n_regionkey', 'n_comment']

    dict_nation_data = [x[1].to_dict() for x in nation_data.iterrows()]

    for nation in dict_nation_data:

        r.hmset("nation:"+str(nation['_id']), {
            'n_nationkey': nation['_id'],
            'n_name': nation['n_name'],
            'n_regionkey': nation['n_regionkey'],
            'n_comment': nation['n_comment'],
        })

    print("FINISHED NATIONS")

## SUPPLIER ##

def importSuppliers():

    supplier_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/supplier.tbl', sep='|', header=None)

    supplier_data.columns = ['_id', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']

    dict_supplier_data = [x[1].to_dict() for x in supplier_data.iterrows()]

    for supplier in dict_supplier_data:

        r.hmset("supplier:"+str(supplier['_id']), {
            's_suppkey': supplier['_id'],
            's_name': supplier['s_name'],
            's_address': supplier['s_address'],
            's_nationkey': supplier['s_nationkey'] ,
            's_phone': supplier['s_phone'],
            's_acctbal': supplier['s_acctbal'],
            's_comment': supplier['s_comment']
        })

    print("FINISHED SUPPLIERS")


## CUSTOMER ##

def importCustomers():

    customer_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/customer.tbl', sep='|', header=None)

    customer_data.columns = ['_id', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']

    dict_customer_data = [x[1].to_dict() for x in customer_data.iterrows()]

    for customer in dict_customer_data:

        result = r.hmset("customer:"+str(customer['_id']), {
            'c_custkey': customer['_id'],
            'c_name': customer['c_name'],
            'c_address': customer['c_address'],
            'c_nationkey': customer['c_nationkey'] ,
            'c_phone': customer['c_phone'],
            'c_acctbal': customer['c_acctbal'],
            'c_mktsegment': customer['c_mktsegment'],
            'c_comment': customer['c_comment']
        })

    print("FINISHED CUSTOMERS")


## PART ##

def importParts():

    part_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/part.tbl', sep='|', header=None)

    part_data.columns = ['_id', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']

    dict_part_data = [x[1].to_dict() for x in part_data.iterrows()]

    for part in dict_part_data:

        r.hmset("part:"+str(part['_id']), {
            'partkey': part['_id'],
            'p_name': part['p_name'],
            'p_mfgr': part['p_mfgr'],
            'p_brand': part['p_brand'] ,
            'p_type': part['p_type'],
            'p_size': part['p_size'],
            'p_container': part['p_container'],
            'p_retailprice': part['p_retailprice'],
            'p_comment': part['p_comment']
        })

    print("FINISHED PARTS")


## PARTSUPP ##

def importPartsupp():

    partsupp_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/partsupp.tbl', sep='|', header=None)

    partsupp_data.columns = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']

    dict_partsupp_data = [x[1].to_dict() for x in partsupp_data.iterrows()]

    for partsupp in dict_partsupp_data:

        r.hmset(
            "partsupp:"+str(partsupp['ps_partkey'])+":"+str(partsupp['ps_suppkey']), 
            {
                'ps_partkey': partsupp['ps_partkey'],
                'ps_suppkey': partsupp['ps_suppkey'],
                'ps_availqty': partsupp['ps_availqty'],
                'ps_supplycost': partsupp['ps_supplycost'] ,
                'ps_comment': partsupp['ps_comment']
            }
        )

    print("FINISHED PARTSSUPPS")

## ORDER ##

def importOrders():

    order_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/orders.tbl', sep='|', header=None)

    order_data.columns = ['_id', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']

    dict_order_data = [x[1].to_dict() for x in order_data.iterrows()]

    for order in dict_order_data:

        orderdate_obj = datetime.strptime(order['o_orderdate'], '%Y-%m-%d')

        r.hmset("order:"+str(order['_id']), {
            'o_orderkey': order['_id'],
            'o_custkey': order['o_custkey'],
            'o_orderstatus': order['o_orderstatus'] ,
            'o_totalprice': order['o_totalprice'],
            'o_orderdate':  str(orderdate_obj.timestamp()),
            'o_orderpriority': order['o_orderpriority'],
            'o_clerk': order['o_clerk'],
            'o_shippriority': order['o_shippriority'],
            'o_comment': order['o_comment']
        })

    print("FINISHED ORDERS")

def importLineitem():

    lineitem_data = pandas.read_csv('../../TPCH-TABLES-SF0.5/lineitem.tbl', sep='|', header=None)

    lineitem_data.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 
                         'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    
    dict_lineitem_data = [x[1].to_dict() for x in lineitem_data.iterrows()]

    for lineitem in dict_lineitem_data:

        shipdate_obj = datetime.strptime(lineitem['l_shipdate'], '%Y-%m-%d')
        commitdate_obj = datetime.strptime(lineitem['l_commitdate'], '%Y-%m-%d')
        receiptdate_obj = datetime.strptime(lineitem['l_receiptdate'], '%Y-%m-%d')

        r.hmset("lineitem:"+str(lineitem['l_orderkey'])+":"+str(lineitem['l_linenumber']), {
            'l_orderkey': lineitem['l_orderkey'],
            'partkey': lineitem['l_partkey'],
            'l_suppkey': lineitem['l_suppkey'] ,
            'l_linenumber': lineitem['l_linenumber'],
            'l_quantity':  lineitem['l_quantity'],
            'l_extendedprice': lineitem['l_extendedprice'],
            'l_discount': lineitem['l_discount'],
            'l_tax': lineitem['l_tax'],
            'l_returnflag': lineitem['l_returnflag'], 
            'l_linestatus': lineitem['l_linestatus'],
            'l_shipdate': str(shipdate_obj.timestamp()),
            'l_commitdate': str(commitdate_obj.timestamp()),
            'l_receiptdate': str(receiptdate_obj.timestamp()),
            'l_shipinstruct': lineitem['l_shipinstruct'],
            'l_shipmode': lineitem['l_shipmode'],
            'l_comment': lineitem['l_comment']
        })
        
        """  r.zadd(
            "lineitem_shipdate_timestamp", 
            {"tpch:lineitem:"+str(lineitem['l_orderkey'])+":"+str(lineitem['l_linenumber']): shipdate_obj.timestamp() * 1000}
        )

        r.zadd(
            "lineitem_commitdate_timestamp", 
            {"tpch:lineitem:"+str(lineitem['l_orderkey'])+":"+str(lineitem['l_linenumber']): commitdate_obj.timestamp() * 1000}
        )

        r.zadd(
            "lineitem_receiptdate_timestamp", 
            {"tpch:lineitem:"+str(lineitem['l_orderkey'])+":"+str(lineitem['l_linenumber']): receiptdate_obj.timestamp() * 1000}
        ) """

    print("FINISHED LINEITEMS")


importRegions()
importNations()
importSuppliers()
importCustomers()
importParts()
importPartsupp()
importOrders()
importLineitem()


