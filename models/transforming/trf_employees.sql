{{ config(materialized = 'table', schema = 'transforming') }}
select
emp.empid,
emp.last_name,
emp.first_name,
emp.title,
emp.hire_date,
iff(mgr.first_name is NULL, emp.first_name, mgr.first_name) as manager,
emp.year_salary,
off.officecity,
off.officecountry,
from
{{ref('stg_employees')}} as emp
left join
{{ref('stg_employees')}} as mgr
on emp.reports_to = mgr.empid
inner join {{ref('lkp_offices')}} as off
on emp.office = off.office