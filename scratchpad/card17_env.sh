# card 17 gate/live env — source this, never echo the values it loads from .env.local
export HUMAUX_TEST_PG_DSN=postgres://postgres:devlocal@127.0.0.1:54329/humaux_thread_dev
export HUMAUX_MAINTENANCE_PG_DSN=postgres://role_maintenance:devlocal_role_maintenance@127.0.0.1:54329/humaux_thread_dev
export HUMAUX_GATEWAY_PG_DSN=postgres://role_gateway:devlocal_role_gateway@127.0.0.1:54329/humaux_thread_dev
export HUMAUX_RETRIEVAL_WORKER_PG_DSN=postgres://role_retrieval_worker:devlocal_role_retrieval_worker@127.0.0.1:54329/humaux_thread_dev
export HUMAUX_TEST_GITLEAKS_BIN=/private/tmp/gitleaks-8.30.1/gitleaks
export HUMAUX_TEST_GITLEAKS_VERSION=8.30.1
export HUMAUX_TEST_GITLEAKS_SHA256=ba52fb1bfabbcde42f032afad3d6e0b19dff8ed105229a16e7caa338bbc0e84f
export HUMAUX_TEST_QDRANT_PORT=6333
# ADR-0039: this node's system DNS is proxy fake-IP hijacked; pin the true public address so the
# checked resolver is also the dial. (Verified via DoH: api.minimaxi.com A 47.79.117.67.)
export HUMAUX_MINIMAX_DNS_PINS=api.minimaxi.com=47.79.117.67
