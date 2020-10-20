create database BigData;

use BigData;

#drop table info;

create table info
(
	Id varchar(200), 
	AgencyName varchar(200),
	ComplaintType varchar(200),
	Borough varchar(200)
);

SET SQL_SAFE_UPDATES = 0;
SHOW VARIABLES LIKE "secure_file_priv"; 

select * from info;

#query

#1
select  ComplaintType, count(*) as cnt from info group by ComplaintType order  by cnt desc limit 1;

#2
select Borough, ComplaintType,  count(ComplaintType) as cnt 
from info as i 
group by Borough, ComplaintType 
having i.ComplaintType = 
(
	select  i2.ComplaintType 
    from info i2 
    where i2.Borough = i.Borough 
    group by i2.ComplaintType 
    order by count(*) desc, i2.ComplaintType limit 1
);

#3
select AgencyName, count(*) as cnt from info group by AgencyName order by cnt desc limit 1;




