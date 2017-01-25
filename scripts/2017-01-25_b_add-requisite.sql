begin transaction;

alter table log rename to log_;

alter table log_ add column requisite int(1);

update log_ set requisite = 1 where path = '/favicon.ico';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.css';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.png';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.jpg';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.woff';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.js';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.gif';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path = '/robots.txt';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.php';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.svg';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.pdf';
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 0 where requisite is null;
select count(*), requisite from log_ group by requisite;

update log_ set requisite = 1 where path like '%.ttf';
select count(*), requisite from log_ group by requisite;

create table log (
  id        integer primary key autoincrement,
  t                            ,   -- seconds since 1970-01-01 (unix epoch) 
  method              not null ,   -- G[ET], P[OST]
  path                not null ,   -- /robots.txt
--HTTP/1.1                     ,   --
  status    int       not null ,   -- 200 
  referrer                     ,   --
  --                           ,   --
  requisite int(1)    not null ,   -- 0, 1
  rogue     int(1)    not null ,   -- 0, 1
  robot                        ,   -- 
  --                           ,   --
  ipnr                not null ,   -- IP Number aaa.bb.ccc.ddd
  fqn                 not null ,
  --
  agent                        ,   --
  size                             --
);

insert into log
select
  id        ,
  t         ,
  method    ,
  path      ,
  status    ,
  referrer  ,
  requisite ,
  rogue     ,
  robot     ,
  ipnr      ,
  fqn       ,
  agent     ,
  size      
from
  log_
;


select count(*) from log_;  
select count(*) from log;  

drop table log_;

commit;
