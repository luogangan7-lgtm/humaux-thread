-- ADR-0063 D-L / "Registry by name" (0231): control.partition_registry ATTACHED rows versus the catalog's leaves of
-- every partitioned table in the schemas $1 (text[]), compared by (leaf name, parent, lower bound, upper bound) in
-- both directions. No relation OID takes part: a logical dump/restore renumbers relations, and the registry must
-- still describe a restored copy (the D-M rollback). One row per drift: (leaf name, problem).
-- Executed by `xtask::rls_check::check_partition_leaves` (the gate) and by the maintenance witness
-- `registry_survives_a_logical_dump_and_restore` (bins/maintenance/tests/partitions.rs), so both read this one text.
WITH cat AS (
  SELECT format('%I.%I', ln.nspname, l.relname) AS leaf_name,
         format('%I.%I', pn.nspname, p.relname) AS parent,
         regexp_match(pg_get_expr(l.relpartbound, l.oid), '^FOR VALUES FROM \((.*)\) TO \((.*)\)$') AS m
    FROM pg_partitioned_table pt
    JOIN pg_class p ON p.oid = pt.partrelid JOIN pg_namespace pn ON pn.oid = p.relnamespace
    JOIN pg_inherits i ON i.inhparent = p.oid
    JOIN pg_class l ON l.oid = i.inhrelid JOIN pg_namespace ln ON ln.oid = l.relnamespace
   WHERE pn.nspname = ANY($1)
), catalog AS (
  SELECT leaf_name, parent,
         CASE WHEN m[1] LIKE '''%' THEN btrim(m[1], '''')::timestamptz END AS lower_bound,
         CASE WHEN m[2] LIKE '''%' THEN btrim(m[2], '''')::timestamptz END AS upper_bound
    FROM cat
), registry AS (
  SELECT g.leaf_name, (SELECT format('%I.%I', n.nspname, c.relname)
                         FROM control.partition_parent(g.table_key) pp
                         JOIN pg_class c ON c.oid = pp.parent
                         JOIN pg_namespace n ON n.oid = c.relnamespace) AS parent,
         g.lower_bound, g.upper_bound
    FROM control.partition_registry g WHERE g.state = 'ATTACHED'
)
SELECT leaf_name, 'leaf with no matching ATTACHED registry row' FROM (SELECT * FROM catalog EXCEPT SELECT * FROM registry) d
UNION ALL
SELECT leaf_name, 'ATTACHED registry row with no matching leaf' FROM (SELECT * FROM registry EXCEPT SELECT * FROM catalog) d
