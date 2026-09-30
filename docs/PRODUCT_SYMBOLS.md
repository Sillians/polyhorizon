# Product symbol allowlist

`polyhorizon/core/product_symbols.py` is the single release-controlled source
of product membership and ordering: **NVDA, AAPL, MSFT**. NVDA is the default.
The broad governed S&P 500 universe remains an upstream eligibility source;
its ordering does not select the product's symbols.

- Ingestion checks that every product symbol exists in the governed snapshot
  and subscribes in product order. Missing symbols or subscription limits below
  the full allowlist cause startup to fail instead of silently reducing coverage.
- Training snapshot and drift readers exclude other symbols. Preprocessing also
  filters independently supplied frames and rejects an empty product dataset.
- Forecast, features, and feature-debug requests reject unsupported symbols
  with HTTP 422. Forecast validation also runs before cache access.
- `/v1/metadata` derives `supported_symbols` from the shared definition. The
  frontend populates its selector from that endpoint; it has no separate list.
  A legacy metadata configuration with a different list fails validation.

To change membership, edit the shared tuple, ensure upstream eligibility and
ingestion capacity, ingest sufficient history, and train/evaluate a model
covering the new symbols. Deploy matching service images together. Membership
alone does not guarantee available history or a trained model for a symbol.
Do not use `MAX_TOTAL_SYMBOLS` or a frontend configuration to redefine membership.
