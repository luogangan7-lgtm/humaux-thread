-- §48 staging.* / public.* canonical tables. public.sources uses the §70.5 DDL verbatim
-- (Public Knowledge pool is tenant-agnostic by design — §5.1 "不要把 public 继续建模成魔法
-- tenant" cuts both ways: it is also not per-tenant data, so no tenant_id/RLS here). The
-- rest are skeleton.

CREATE TABLE staging.contribution_releases (
  contribution_release_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id                 uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  created_at                timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE staging.contribution_release_sources (
  contribution_release_id uuid NOT NULL REFERENCES staging.contribution_releases(contribution_release_id),
  memory_id                 uuid NOT NULL REFERENCES private.memory_records(memory_id),
  PRIMARY KEY (contribution_release_id, memory_id)
);

-- §70.5, verbatim.
CREATE TABLE public.sources (
  source_id              uuid PRIMARY KEY DEFAULT uuidv7(),
  source_type            text NOT NULL,
  publisher              text,
  source_url             text,
  content_hash           text NOT NULL,
  source_license         text,
  rights_basis           text NOT NULL,
  redistribution_policy  text,
  trust_class            text NOT NULL,
  retrieved_at           timestamptz,
  created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.knowledge_gaps (
  knowledge_gap_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  description        text        NOT NULL,
  created_at         timestamptz NOT NULL DEFAULT now()
);

-- §70.5: "所有 public.claims 必须至少有一个有效 source/provenance parent" — enforced at the
-- application layer alongside public.provenance_edges (link-table pattern, no array
-- second-source); this table just carries the claim body.
CREATE TABLE public.claims (
  claim_id   uuid        PRIMARY KEY DEFAULT uuidv7(),
  content    jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.syntheses (
  synthesis_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  content       jsonb       NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.topics (
  topic_id   uuid        PRIMARY KEY DEFAULT uuidv7(),
  name       text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.relations (
  from_claim_id uuid        NOT NULL REFERENCES public.claims(claim_id),
  to_claim_id   uuid        NOT NULL REFERENCES public.claims(claim_id),
  relation_kind text        NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (from_claim_id, to_claim_id, relation_kind)
);

CREATE TABLE public.provenance_edges (
  claim_id   uuid        NOT NULL REFERENCES public.claims(claim_id),
  source_id  uuid        NOT NULL REFERENCES public.sources(source_id),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (claim_id, source_id)
);

CREATE TABLE public.source_closure (
  claim_id        uuid        NOT NULL REFERENCES public.claims(claim_id),
  root_source_id  uuid        NOT NULL REFERENCES public.sources(source_id),
  computed_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (claim_id, root_source_id)
);
