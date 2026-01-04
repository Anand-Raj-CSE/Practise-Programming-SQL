--Question:
--Table: orders(order_id, customer_id, order_date, amount)
--👉 Find customers who have placed more than 5 orders.
Select customer_id from orders group by customer_id having count(order_id)>5


/*⚡ SQL Quiz – Q6
Question:
Table: orders(order_id, customer_id, order_date, amount)
👉 Write a query to get cumulative (running) total of order amounts per customer, 
ordered by order_date.
*/
Select customer_id,sum(amount) from orders 
group by customer_id , order_date order by order_date desc --- not runnig sum
--correct 
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM orders
ORDER BY customer_id, order_date;

/*⚡ SQL Quiz – Q7

Question:Write a query to get the top 2 highest orders (by amount) per customer.
Table: orders(order_id, customer_id, order_date, amount)
 */
 Select order_id , dense_rank() OVER (PARTITION by customer_id ORDER By amount) as rn 
 from orders where rn >=2      -- again incorrect we cannot use rn in where as where is executed first.
 -- Correct query 
  Select order_id , customer_id,amount FROM (SELECT order_id ,customer_id,amount, dense_rank()
   OVER (PARTITION by customer_id ORDER By amount DESC) as rn from orders) t
   where rn<=2

/*SQL Quiz – Q8
Table: orders(order_id, customer_id, order_date, amount)
👉 Find customers who placed orders in January 2025 but did not place any orders in February 2025. */
WITH A as (
    SELECT customer_id from order where month(order_date) = 1 and year(order_date) = 2025
), B as (
    SELECT customer_id from order where month(order_date) =2 and year(order_date) = 2025
)

Select * from A where customer_id not in (Select customer_id from B)

/*SQL Quiz – Q9
Table: orders(order_id, customer_id, order_date, amount)
👉 Write a query to get cumulative (running) total of order amounts per customer, 
ordered by order_date. */
Select customer_id,order_id,
    sum(amount) over (PARTITION by customer_id order by order_date DESC) as t from orders
    order by customer_id,order_date

/*SQL Quiz – Q11
Question:
Table: orders(order_id, customer_id, order_date, amount)
👉 Find the customers whose cumulative order amount exceeded 10,000
 at any point, and show the first date this happened per customer. */
 with cte as (
    Select customer_id,amount, order_date , sum(amount) over 
    (PARTITION BY customer_id order by order_date) as t from orders
 )
 Select customer_id,amount, min(order_date)  from cte where t > 10000 group by customer_id


 ---------------------------------------- Organizational recursive CTE question-----------------------

 with orgLvl as (
    Select emp_id as Employee_id ,manager_id as Manager_emp_id , 
    1 as lvl from emp 
    where manager_id is NULL

    UNION ALL

    Select e.emp_id as Employee_id , o.Employee_id as Manager_emp_id , o.lvl+1 as lvl 
    from emp e left join orgLvl o on e.manager_id = o.Employee_id
 )

 Select * from orgLvl where lvl <= 3

-- We should use , inner join in case of recursive cte not left join , and sometimes it could go into infinite,
-- loop so to prevent we could add a condition in recursive anchor query as well. So correct code for all 
-- hierrachy till 3 is : 
 with orgLvl as (
    Select emp_id as Employee_id ,manager_id as Manager_emp_id , 
    1 as lvl from emp 
    where manager_id is NULL

    UNION ALL

    Select e.emp_id as Employee_id , e.manager_id as Manager_emp_id , o.lvl+1 as lvl 
    from emp e join orgLvl o on e.manager_id = o.Employee_id where o.lvl < 3
 )
 Select * from orgLvl where lvl <= 3 order by lvl ,Employee_id, Manager_emp_id


 -----------------runnig sum
 with cte as (
    Select cust_id , purchase_dt ,
    sum(amt) over(PARTITION By cust_id order by purchase_dt) rows between unboundedPreceding and currentRow as running sum
    from customer
 )
 Select * from cte