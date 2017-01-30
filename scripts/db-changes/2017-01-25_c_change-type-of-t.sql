begin transaction;

alter table log rename to log_;

create table log (
  id        integer primary key autoincrement,
  t         int       not null ,   -- seconds since 1970-01-01 (unix epoch) 
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
  size     int                     --
);

drop   index log_t_ix;
create index log_t_ix on log (t);

select count(*), typeof(t) from log group by typeof(t);

drop table log_;

commit;
