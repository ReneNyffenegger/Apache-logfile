begin transaction;

alter table log rename to log_;

create table log as
select
  l.id,
  l.t,
  l.method,
  l.path,
  l.status,
  l.referrer,
  l.rogue,
  l.robot,
  l.ipnr,
  i.fqn,
  l.agent,
  l.size
from
  log_ l    join
  ip   i on l.ipnr = i.ipnr
;

select count(*) from log_;  
select count(*) from log;  

drop table log_;

commit;
