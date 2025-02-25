use database inter;
create or replace table employee(
    id int ,
    name varchar,
    salary int,
    manager_id int
)
;
insert into employee values
(1,'joe',70000,3),
(2,'henry',80000,4),
(3,'sam',60000,null),
(4,'max',90000,null);

select * from employee;
-- get the employess who are getting more salary than thier manager
select e.name from employee e
join employee m
on e.manager_id = m.id
where e.salary > m.salary;

