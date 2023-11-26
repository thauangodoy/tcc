from pymongo import MongoClient
import pandas

#MONGODB CONNECTION SETUP#

mongo_client = MongoClient('localhost', 27017)
mongodb = mongo_client.tpch1

## REGION ##

def importRegions():

    region = mongodb.region

    region_data = pandas.read_csv('../../TPCH-TABLES-SF1/region.tbl', sep='|', header=None)

    region_data.columns = ['r_regionkey', 'r_name', 'r_comment']
    dict_region_data = [x[1].to_dict() for x in region_data.iterrows()]

    region.insert_many(dict_region_data)

    print("FINISHED REGION")

## NATION ##

def importNations():

    nation = mongodb.nation

    nation_data = pandas.read_csv('../../TPCH-TABLES-SF1/nation.tbl', sep='|', header=None)

    print(nation_data)

    nation_data.columns = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']

    dict_nation_data = [x[1].to_dict() for x in nation_data.iterrows()]

    nation.insert_many(dict_nation_data)

    print("FINISHED NATION")


## SUPPLIER ##

def importSuppliers():

    supplier = mongodb.supplier

    supplier_data = pandas.read_csv('../../TPCH-TABLES-SF1/supplier.tbl', sep='|', header=None)

    supplier_data.columns = ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']

    dict_supplier_data = [x[1].to_dict() for x in supplier_data.iterrows()]

    supplier.insert_many(dict_supplier_data)

    print("FINISHED SUPPLIER")

## CUSTOMER ##

def importCustomers():

    customer = mongodb.customer

    customer_data = pandas.read_csv('../../TPCH-TABLES-SF1/customer.tbl', sep='|', header=None)

    customer_data.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']

    dict_customer_data = [x[1].to_dict() for x in customer_data.iterrows()]

    customer.insert_many(dict_customer_data)

    print("FINISHED CUSTOMER")

## PART ##

def importPart():

    part = mongodb.part

    part_data = pandas.read_csv('../../TPCH-TABLES-SF1/part.tbl', sep='|', header=None)

    part_data.columns = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']

    dict_part_data = [x[1].to_dict() for x in part_data.iterrows()]

    part.insert_many(dict_part_data)

    print("FINISHED PART")
    

## PARTSUPP ##

def importPartsupp():

    partsupp = mongodb.partsupp

    partsupp_data = pandas.read_csv('../../TPCH-TABLES-SF1/partsupp.tbl', sep='|', header=None)

    partsupp_data.columns = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']

    dict_partsupp_data = [x[1].to_dict() for x in partsupp_data.iterrows()]

    partsupp.insert_many(dict_partsupp_data)

    print("FINISHED PARTSUPP")

## ORDERS ##

def importOrders():

    order = mongodb.order

    order_data = pandas.read_csv('../../TPCH-TABLES-SF1/orders.tbl', sep='|', header=None)

    order_data.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']

    order_data['o_orderdate'] = pandas.to_datetime(order_data['o_orderdate'])

    dict_order_data = [x[1].to_dict() for x in order_data.iterrows()]

    order.insert_many(dict_order_data)

    print("FINISHED ORDER")

## LINEITEM ##

def importLineitems():

    lineitem = mongodb.lineitem

    chunksize = 100000
    for chunk in pandas.read_csv('../../TPCH-TABLES-SF1/lineitem.tbl', sep='|', header=None, chunksize=chunksize):

        chunk.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 
                         'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
        
        chunk['l_shipdate'] = pandas.to_datetime(chunk['l_shipdate'])
        chunk['l_commitdate'] = pandas.to_datetime(chunk['l_commitdate'])
        chunk['l_receiptdate'] = pandas.to_datetime(chunk['l_receiptdate'])
        
        dict_lineitem_data = [x[1].to_dict() for x in chunk.iterrows()]
        lineitem.insert_many(dict_lineitem_data)

    print("FINISHED LINEITEM")

importRegions()
importNations()
importSuppliers()
importCustomers()
importPart()
importPartsupp()
importOrders()
importLineitems()

