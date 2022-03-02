
FILENAME REFFILE '/home/u45131587/EPG1V2/data/transaction_dat.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.transaction_dat;
	GETNAMES=YES;
RUN;



FILENAME REFFILE '/home/u45131587/EPG1V2/data/account_dat.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.account_dat;
	GETNAMES=YES;
RUN;


FILENAME REFFILE '/home/u45131587/EPG1V2/data/category_ref.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.category_ref;
	GETNAMES=YES;
RUN;


FILENAME REFFILE '/home/u45131587/EPG1V2/data/in-app_dat.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.in_app_dat;
	GETNAMES=YES;
RUN;

FILENAME REFFILE '/home/u45131587/EPG1V2/data/app_dat.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=WORK.app_dat;
	GETNAMES=YES;
RUN;

proc sql outobs=5;
select * from in_app_dat;
run;

proc sql outobs=5;
select * from app_dat;
run;
proc sql;
select * from app_dat
where content_id in ('d2bde35599e0dae9');
run;

proc sql;
select * from in_app_dat
where parent_app_content_id in ('d2bde35599e0dae9');
run;
proc sql;
select * from account_dat
where acct_id in ('2269371e31db2263');
run;
proc sql;
select * from transaction_dat
where content_id in ('227227a77c59809b');
run;
proc sql;
create table acct_view as
select acct_id,count(*) as cnt from transaction_dat
group by 1
order by 2;
run;
proc sql;
create table acct_fraud as
select acct_id ,cnt from acct_view
where cnt>1000;
run;

proc sql;
select month(create_dt),year(create_dt),count(distinct acct_id) from account_dat
group by 1,2
order by 2 ,1;
run;

proc sql;
select payment_type,count(distinct acct_id) from account_dat
group by 1
order by 1;
run;
data transaction_dat2(drop=create_dt);
set transaction_dat;
newdate = input(put(create_dt,10.),yymmdd10.);
format newdate date10.;
run;

data transaction_dat3;
set transaction_dat;
newdate = input(put(create_dt,10.),yymmdd10.);
format newdate date10.;
run;
proc sql;
select month(newdate),year(newdate),sum(price) from transaction_dat2
group by 1,2
order by 2 ,1;
run;
proc sql;
select device_id,sum(price) from transaction_dat2
group by 1
order by 1;
run;
proc sql;
select month(newdate),year(newdate),count(distinct acct_id) from transaction_dat2
group by 1,2
order by 2 ,1;
run;
proc sql;
select month(newdate),year(newdate),count(distinct content_id)  from transaction_dat2
group by 1,2
order by 2 ,1;
run;


proc sql;
create table app_cat as 
select a.*,b.category_name from app_dat a
left join category_ref b on a.category_id=b.category_id;
run;



proc sql;
create table in_app_app_cat as 
select a.*,b.* from in_app_dat a
left join app_cat b on a.parent_app_content_id=b.content_id;
run;

proc sql;
create table trns_acct as 
select a.*,b.* from transaction_dat2 a
left join account_dat b on a.acct_id=b.acct_id;
run;

proc sql;
create table tran_acct_app_cat as 
select a.*,b.* from trns_acct a
left join in_app_app_cat b on a.content_id=b.content_id;
run;
proc sql;
create table subs as 
select * from tran_acct_app_cat
where type in ('subscription');
run;
proc sql;
create table subs_acct as 
select * from subs

where acct_id in ('6862a0c86566fdb5')
order by app_name,newdate;
run;
proc sql;
create table x as
select * from tran_acct_app_cat
where year(create_dt)=2017;
run;
proc sql;
create table x as
select * from tran_acct_app_cat
where create_dt>newdate  ;
run;
proc sql;
create table x1 as
select * from tran_acct_app_cat
where create_dt<newdate  and app_name is null ;
run;
proc sql;
create table x2 as
select * from tran_acct_app_cat
where  app_name is null ;
run;
proc sql;
create table acct_fraud as
select acct_id ,cnt from acct_view
where acct_id in (select acct_id from x);
run;
proc sql;
create table acct_fraud as
select acct_id ,cnt from acct_view
where acct_id in (select acct_id from x1);
run;
proc sql;
create table tran_acct_fraud as 
select * from tran_acct_app_cat a
where acct_id in ( select acct_id from x) ;
run;
proc sql;
create table fraud_app as 
select * from tran_acct_app_cat a
where app_name  is not null
order by create_dt;
run;
proc sql;
create table exp_aact as 
select * from tran_acct_app_cat a
where APP_NAME in ("medium carpenter's") and acct_id in ('ec2889d90f3a6505');
order by acct_id ;
run;

proc sql;
create table subscription_days as 
select acct_id,app_name, max(newdate) - min(newdate) as days_used from tran_acct_app_cat a
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('subscription')
group by 1,2 
order by 3;
run;

proc sql;
create table avg_subscription_days as 
select app_name, avg(days_used) as avg from subscription_days a
group by 1
order by 2
;
run;

proc sql;

select app_name,count(distinct acct_id) as accts from tran_acct_app_cat a
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('subscription')
group by 1
order by 2;	
run;
proc sql;
create table cons_days as 
select acct_id,app_name, max(newdate) - min(newdate) as days_used from tran_acct_app_cat 
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('consumable')
group by 1,2 
order by 3;
run;

proc sql;

select app_name, avg(days_used) as avg from cons_days 
group by 1
order by 2
;
run;
proc sql;

select app_name,count(distinct acct_id) as accts from tran_acct_app_cat a
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('consumable')
group by 1
order by 2;	
run;

proc sql;

select month(newdate),app_name,count(distinct acct_id) as accts from tran_acct_app_cat a
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('consumable')
group by 1,2
order by 1;	
run;

proc sql;

select month(newdate),app_name,count(distinct acct_id) as accts from tran_acct_app_cat a
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('subscription')
group by 1,2
order by 1;	
run;
proc sql;
create table sub as
select * from tran_acct_app_cat
where APP_NAME is not null and APP_NAME not in ('#NAME?') and type in ('subscription');
run;
proc sql;
create table daysbetweensubs as 
select a.*,a.newdate as date1, b.newdate as date2,a.newdate-b.newdate as days from sub a
left join sub b on a.acct_id=b.acct_id and a.content_id=b.content_id  ;
run;
proc sql;
create table daysbetweensubs2 as
select * from daysbetweensubs
where days>=0;
run;

proc sql;
create table z as
select app_name,avg(days),max(days) from daysbetweensubs2
group by 1;
run;
proc sql;
select type,count(distinct parent_app_content_id) ,count(distinct content_id)  from in_app_app_cat
group by 1;
run;
proc sql;
select device_id,count(distinct parent_app_content_id) ,count(distinct content_id)  from in_app_app_cat
group by 1;
run;

proc sql;
select category_name,count(distinct parent_app_content_id) ,count(distinct content_id)  from in_app_app_cat
group by 1;
run;

proc sql;

select app_name,sum(price) as sum_price from tran_acct_app_cat
group by 1;
run;
proc sql;

select payment_type,count(distinct acct_id)as acct,count(distinct content_id)as cnt_id,sum(price) as sum_price
group by 1;
run;
proc sql;

select category_name,count(distinct acct_id)as acct,count(distinct content_id)as cnt_id,sum(price) as sum_price as sum_price from tran_acct_app_cat
group by 1;
run;
proc sql;

select type,count(distinct acct_id)as acct,count(distinct content_id)as cnt_id,sum(price) as sum_price as sum_price from tran_acct_app_cat
group by 1;
run;
proc sql;

select app_name,count(distinct acct_id)as acct,count(distinct content_id)as cnt_id,sum(price) as sum_price from tran_acct_app_cat
group by 1
order by 1;
run;
