create database BigData;

use BigData;


create table info
(
	ComplaintType varchar(200),
	AgencyName varchar(200),
	Borough varchar(200)
);



select * from info;
insert into info values ('Test', 'Test', 'Test');




select  ComplaintType, count(*) as cnt from info group by ComplaintType order  by cnt desc limit 1;





