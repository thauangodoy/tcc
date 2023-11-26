FT.CREATE idx:region ON HASH PREFIX 1 "region:" SCHEMA
    r_regionkey NUMERIC SORTABLE 
    r_name TEXT SORTABLE 
    r_comment TEXT

FT.CREATE idx:nation ON HASH PREFIX 1 "nation:" SCHEMA
    n_nationkey NUMERIC SORTABLE 
    n_name TEXT SORTABLE 
    n_regionkey NUMERIC SORTABLE 
    n_comment TEXT


FT.CREATE idx:supplier ON HASH PREFIX 1 "supplier:" SCHEMA
    s_suppkey NUMERIC SORTABLE 
    s_name TEXT SORTABLE 
    s_address TEXT SORTABLE 
    s_nationkey NUMERIC SORTABLE 
    s_phone TEXT SORTABLE 
    s_acctbal NUMERIC SORTABLE 
    s_comment TEXT

FT.CREATE idx:customer ON HASH PREFIX 1 "customer:" SCHEMA
    c_custkey NUMERIC SORTABLE
    c_name TEXT SORTABLE
    c_address TEXT SORTABLE
    c_nationkey NUMERIC SORTABLE
    c_phone TEXT SORTABLE
    c_acctbal NUMERIC SORTABLE
    c_mktsegment TEXT SORTABLE
    c_comment TEXT

FT.CREATE idx:part ON HASH PREFIX 1 "part:" SCHEMA
    p_partkey NUMERIC SORTABLE
    p_name TEXT SORTABLE
    p_mfgr TEXT SORTABLE
    p_brand TEXT SORTABLE
    p_type TEXT SORTABLE
    p_size NUMERIC SORTABLE
    p_container TEXT SORTABLE
    p_retailprice NUMERIC SORTABLE
    p_comment TEXT

FT.CREATE idx:partsupp ON HASH PREFIX 1 "partsupp:" SCHEMA
    ps_partkey NUMERIC SORTABLE
    ps_suppkey NUMERIC SORTABLE
    ps_availqty NUMERIC SORTABLE
    ps_supplycost NUMERIC SORTABLE
    ps_comment TEXT

FT.CREATE idx:order ON HASH PREFIX 1 "order:" SCHEMA
    o_orderkey NUMERIC SORTABLE
    o_custkey TEXT SORTABLE
    o_orderstatus TEXT SORTABLE
    o_totalprice TEXT SORTABLE
    o_orderdate TEXT SORTABLE
    o_orderpriority NUMERIC SORTABLE
    o_clerk TEXT SORTABLE
    o_shippriority NUMERIC SORTABLE
    o_comment TEXT

FT.CREATE idx:lineitem ON HASH PREFIX 1 "lineitem:" SCHEMA
    l_orderkey NUMERIC SORTABLE
    l_partkey NUMERIC SORTABLE
    l_suppkey NUMERIC SORTABLE
    l_linenumber NUMERIC SORTABLE
    l_quantity NUMERIC SORTABLE
    l_extendedprice NUMERIC SORTABLE
    l_discount NUMERIC SORTABLE
    l_tax NUMERIC SORTABLE
    l_returnflag TEXT SORTABLE
    l_linestatus TEXT SORTABLE
    l_shipdate TEXT SORTABLE
    l_commitdate TEXT SORTABLE
    l_receiptdate TEXT SORTABLE
    l_shipinstruct TEXT SORTABLE
    l_shipmode TEXT SORTABLE
    l_comment TEXT



