{{config(materialized = 'view', schema = 'reporting')}}
 
select
c.companyname,
c.contactname,
c.city,
sum(f.linesaleamount) as sales,
sum(f.quantity) as quantity,
avg(f.margin) as margin
from
{{ref("dim_customers")}} as c
inner join {{ref("fct_orders")}} as f
on c.customerid = f.customerid
where c.city = {{var('vcity',"'London'")}}
group by c.companyname,
c.contactname,
c.city
order by sales desc