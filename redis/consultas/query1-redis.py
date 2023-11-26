"""   FT.AGGREGATE idx:lineitem "*"
    FILTER "@l_shipdate <= [906346800.0]" 
    TIMEOUT 1000000
    APPLY "(@l_extendedprice * (1 - @l_discount))" AS sum_disc_price
    APPLY "(@l_extendedprice * (1 - @l_discount) * (1 + @l_tax))" AS sum_charge
    GROUPBY 2 @l_returnflag @l_linestatus
    REDUCE AVG 1 @l_quantity AS avg_qty
    REDUCE AVG 1 @l_extendedprice AS avg_price
    REDUCE AVG 1 @l_discount AS avg_disc
    REDUCE FIRST_VALUE 1 @sum_disc_price AS sum_disc_price
    REDUCE FIRST_VALUE 1 @sum_charge AS sum_charge
    REDUCE COUNT 0 AS count_order
    SORTBY 4 @l_returnflag ASC @l_linestatus ASC  """