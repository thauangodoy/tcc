""" FT.AGGREGATE idx:lineitem "*"
    FILTER "@l_shipdate>=[757382400.0] && @l_shipdate < [757382400.0] && @l_discount >= 0.07 && @l_discount <= 0.09 && @l_quantity < 24"
    TIMEOUT 10000000
    APPLY "(@l_extendedprice * @l_discount)" AS revenue
    REDUCE SUM 1 @revenue AS total_revenue """