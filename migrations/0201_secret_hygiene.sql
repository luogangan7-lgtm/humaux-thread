-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0200). Card 33 / ADR-0059 D-A, D-C;
-- audit SEC-2.
--
-- D-A: role_migration_owner never connects. Every migration runs as the migrate principal and hands
-- objects over with ALTER ... OWNER TO; SET ROLE role_migration_owner needs no LOGIN. The owner is
-- therefore NOLOGIN with no stored verifier, so a later ALTER ROLE ... LOGIN alone still grants no
-- password login. This supersedes 0011's header sentence "LOGIN is required" for the owner only
-- (§6.2.0 / §48.2 role-set equality now expects the owner NOLOGIN and the other eight LOGIN).
--
-- D-C (SEC-2): a runtime role that can INSERT a row into ops.schema_migrations can make migrate skip
-- a hardening migration. The table is owner-only. The five ops.email_* tables leave the ops.* R+W
-- domain default (which let every worker forge outbox rows) and keep exactly the verbs
-- adapters::email::outbox issues: enqueue as role_gateway, run_once / record_suppression as
-- role_private_worker. ops.email_domains and ops.email_provider_health have no code writer.

ALTER ROLE role_migration_owner NOLOGIN PASSWORD NULL;

REVOKE ALL ON ops.schema_migrations FROM
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

REVOKE ALL ON ops.email_outbox, ops.email_delivery_events, ops.email_suppressions,
  ops.email_domains, ops.email_provider_health FROM
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

-- outbox::enqueue: one INSERT without RETURNING; is_suppressed: one SELECT.
GRANT INSERT ON ops.email_outbox TO role_gateway;
GRANT SELECT ON ops.email_suppressions TO role_gateway;
-- outbox::run_once: SELECT ... FOR UPDATE SKIP LOCKED + UPDATE, one delivery-event INSERT per outcome;
-- record_suppression: INSERT ... ON CONFLICT (email, scope) DO UPDATE.
GRANT SELECT, UPDATE ON ops.email_outbox TO role_private_worker;
GRANT INSERT ON ops.email_delivery_events TO role_private_worker;
GRANT SELECT, INSERT, UPDATE ON ops.email_suppressions TO role_private_worker;
