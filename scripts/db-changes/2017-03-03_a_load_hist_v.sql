drop view if exists load_hist_v;
create view load_hist_v as
select
  datetime(t_load_start       , 'unixepoch') load_start,
  datetime(t_load_end         , 'unixepoch') load_end,
  datetime(max_t_at_load_start, 'unixepoch') max_t_at_load_start
from
  load_hist
order by
  t_load_start;
