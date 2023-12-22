--bai 1
with cte as
(select *,
rank() over (partition by customer_id order by order_date asc) as rk
from Delivery)

select 
Round(sum(case when order_date=customer_pref_delivery_date then 1 else 0 end)/count(delivery_id)*100.0,2) as immediate_percentage
from 
(select customer_id, delivery_id, order_date,customer_pref_delivery_date
from cte
where rk = 1
group by customer_id) as delivery_count
--bai 2
WITH CTE AS 
(SELECT
player_id, min(event_date) as event_start_date
from
Activity
group by player_id)
SELECT 
Round(count(distinct a.player_id)/(select count(distinct player_id) from Activity),2) as fraction
From CTE a JOIN Activity b
on a.player_id=b.player_id
where DATEDIFF(a.event_start_date,b.event_date)=-1
--bai 3
select
(case when id%2 != 0 and counts != id then id + 1
when id%2 != 0 and counts = id then id else id - 1 end) as id,
student
from seat, (select count(*) as counts from Seat) as seat_counts
ORDER BY id asc
--bai 4
with cte as
(SELECT DISTINCT visited_on, 
SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN INTERVAL 6 DAY PRECEDING
AND CURRENT ROW) AS amount
FROM Customer)
select visited_on, amount, ROUND(amount/7, 2) as average_amount
From cte a JOIN (select Min(visited_on) as min_visited_on from cte) b
on datediff(a.visited_on,b.min_visited_on)>=6
--bai 5
select sum(tiv_2016) as tiv_2016
from Insurance
where tiv_2015 in 
(select TIV_2015 
from insurance
group by 1
having count(*) > 1) and 
concat(LAT, LON) in
(select concat(LAT,LON) 
from insurance
group by LAT, LON
having count(*) = 1)
--bai 6
with cte as
(Select a.departmentID, b.name as Department, a.name as Employee, a.Salary,
dense_rank() over(partition by b.name order by a.Salary desc) as rk
from Employee a JOIN Department b
on a.departmentId=b.id)
select cte.Department, Employee, Salary 
from cte
where rk <=3
order by departmentID DESC
--bai 7
with cte as
(Select turn, person_id as ID, person_name as Name, Weight,
Sum(Weight) over (Order by turn) as Total_Weight
FROM Queue)
select Name as person_name from cte
where Total_Weight =1000
--bai 8
select * from 
(select product_id, new_price as price 
from Products
where (product_id, change_date) in 
(select product_id, max(change_date) from Products
where change_date <= '2019-08-16'
group by product_id)
union
select distinct product_id, 10 as price 
from Products
where product_id not in 
(select product_id from Products
where change_date <= '2019-08-16'))
union_result;
