select
orderid,
sum(LINESALEAMOUNT) as sales
from
{{ref('fct_orders')}}
group by orderid
having sales < 0