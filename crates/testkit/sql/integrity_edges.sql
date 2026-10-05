-- Integrity edges of the connected database, read from the catalog at run time (ADR-0063 "Dev integrity finding").
-- One row per edge, no table named:
--   kind 'fk'       every FOREIGN KEY, per-leaf clones included (`inherited`); MATCH SIMPLE, so a row with any NULL
--                   key column is never a violation;
--   kind 'identity' ADR-0063 D-B: a plain table whose primary key is a partitioned parent's primary key minus the
--                   partition key and whose every column is a same-typed column of that parent (child = the identity
--                   table, parent = the partitioned table). Its row whose parent row is gone is garbage, unless the
--                   row's partition key lies in a DROPPED `control.partition_registry` range: a tombstone (D-B
--                   invariant 3, L1).
-- scan_sql prints `child|parent|constraint|n` (fk) or `identity|table|parent|n` when n > 0 rows violate the edge;
-- delete_sql deletes exactly those rows and prints the count.
-- Read by crates/testkit/src/fixture_purge.rs (include_str!) and by the task-work script c36_dev_orphan_repair.sh.
with fk as (
  select c.conrelid as child, c.confrelid as parent, c.conname::text as label, c.conparentid <> 0 as inherited,
         array(select a.attname::text from unnest(c.conkey) with ordinality k(n, i)
                 join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.n order by k.i) as child_cols,
         array(select a.attname::text from unnest(c.confkey) with ordinality k(n, i)
                 join pg_attribute a on a.attrelid = c.confrelid and a.attnum = k.n order by k.i) as parent_cols
    from pg_constraint c
   where c.contype = 'f'
),
pk as (
  select c.conrelid as rel,
         array(select a.attname::text from unnest(c.conkey) k(n)
                 join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.n order by 1) as cols
    from pg_constraint c
   where c.contype = 'p'
),
ident as (
  select i.rel as child, pt.partrelid as parent, i.cols,
         (select a.attname::text from pg_attribute a
           where a.attrelid = pt.partrelid and a.attnum = pt.partattrs[0]) as key_col
    from pg_partitioned_table pt
    join pg_class pc on pc.oid = pt.partrelid and not pc.relispartition
    join pk pp on pp.rel = pt.partrelid
    join pk i on i.cols = array(select x from unnest(pp.cols) x
                                 where x <> all (array(select a.attname::text from pg_attribute a
                                                        where a.attrelid = pt.partrelid
                                                          and a.attnum = any (pt.partattrs)))
                                 order by 1)
    join pg_class ic on ic.oid = i.rel and ic.relkind = 'r' and not ic.relispartition
   where not exists (select 1 from pg_attribute ia
                      where ia.attrelid = i.rel and ia.attnum > 0 and not ia.attisdropped
                        and not exists (select 1 from pg_attribute pa
                                         where pa.attrelid = pt.partrelid and pa.attname = ia.attname
                                           and pa.atttypid = ia.atttypid and not pa.attisdropped))
),
edges as (
  select 'fk' as kind, child, parent, label, inherited, child_cols, parent_cols, '' as tombstone from fk
  union all
  select 'identity', child, parent, child::regclass::text, false, cols, cols,
         case when to_regclass('control.partition_registry') is not null
                   and exists (select 1 from pg_attribute a
                                where a.attrelid = ident.child and a.attname = ident.key_col and not a.attisdropped)
              then format(' and not exists (select 1 from control.partition_registry r where r.state = ''DROPPED'''
                          ' and r.table_key in (select r2.table_key from control.partition_registry r2'
                          ' join pg_inherits h on h.inhrelid = to_regclass(r2.leaf_name) where h.inhparent = %L::regclass)'
                          ' and c.%I >= coalesce(r.lower_bound, ''-infinity'') and c.%I < r.upper_bound)',
                          ident.parent::regclass::text, ident.key_col, ident.key_col)
              else '' end
    from ident
)
select e.kind, e.child::regclass as child, e.parent::regclass as parent, e.label, e.inherited,
       e.child_cols, e.parent_cols,
       array(select format_type(a.atttypid, a.atttypmod) from unnest(e.child_cols) with ordinality k(n, i)
               join pg_attribute a on a.attrelid = e.child and a.attname = k.n order by k.i) as child_types,
       format('select %L||''|''||count(*) from %s c where %s and not exists (select 1 from %s p where %s)%s'
              ' having count(*) > 0;',
              case e.kind when 'fk' then format('%s|%s|%s', e.child::regclass, e.parent::regclass, e.label)
                          else format('identity|%s|%s', e.child::regclass, e.parent::regclass) end,
              e.child::regclass, m.nn, e.parent::regclass, m.pred, e.tombstone) as scan_sql,
       format('with d as (delete from %s c where %s and not exists (select 1 from %s p where %s)%s returning 1)'
              ' select count(*) from d;',
              e.child::regclass, m.nn, e.parent::regclass, m.pred, e.tombstone) as delete_sql
  from edges e
  cross join lateral (
    select string_agg(format('c.%I is not null', u.cc), ' and ' order by u.i) as nn,
           string_agg(format('c.%I = p.%I', u.cc, u.pc), ' and ' order by u.i) as pred
      from unnest(e.child_cols, e.parent_cols) with ordinality u(cc, pc, i)
  ) m
