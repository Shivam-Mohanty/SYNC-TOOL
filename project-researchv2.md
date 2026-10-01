# Collaborative AI Context Synchronization Platform
## Refined Architecture Document — v2
> Generated from design review session. Supersedes `project-researchv1.md`.

---

## 1. Executive Summary & Core Concept

- **Problem**: Dev teams using fragmented AI chat interfaces (Gemini, ChatGPT, Claude) across separate accounts suffer from missing context, no team memory, and zero synchronization. This causes poor output, hallucination, and repeated prompting costs.
- **Core Solution**: A collaborative platform functioning as a distributed semantic version control system for AI context. Tracks architectural decisions, constraints, and task states across a team in a unified knowledge graph.
- **The "Queen Agent" Model**: The web platform acts as the strategic brain and memory coordinator (architect, task planner, and context orchestrator). Individual developers and coding agents handle tactical implementation.

---

## 2. Ingestion Strategy (Phased)

| Phase | Mechanism | Notes |
|---|---|---|
| **v1** | Manual import pipeline | Developers paste/upload prior context or docs; extraction worker processes into graph on demand |
| **v2** | Git integration | Parse PRs, commit messages, and code comments as a secondary ingestion source |
| **v3** | Browser extension | Intercept LLM chat sessions (ChatGPT, Claude.ai) in the browser to broaden capture scope |

> **Cold-start gap acknowledged**: The platform only captures context routed through it by default. Phased ingestion progressively closes this gap without blocking v1 launch.

---

## 3. Proxy Enforcement Model

**Hybrid enforcement**:
- The API Gateway proxy is **mandatory** for all team-shared context (workspace-scoped calls).
- Personal and exploratory calls can bypass the proxy freely.
- A **"Promote" flag** allows developers to retroactively surface useful personal-call results into the shared workspace context.

> **Incentive model**: Value flows from keeping shared context synchronized; bypass is permitted but opt-in promotion ensures valuable insights aren't permanently lost.

---

## 4. Security Architecture

### 4.1 BYOK Key Lifecycle (Envelope Encryption)

```
Developer submits API key
        |
        v
[ Encrypt with per-workspace DEK (AES-256) ]
        |
        v
[ DEK encrypted by master KEK in Cloud KMS / HashiCorp Vault ]
        |
        v
[ Encrypted DEK stored in PostgreSQL ]

--- At call time ---

[ Gateway fetches DEK from KMS ] → [ Decrypts API key in memory ]
        |
        v
[ Proxied LLM call executes ]
        |
        v
[ DEK evicted from memory immediately post-call ]
```

**Key properties**:
- Raw API key exists in plaintext **only during the proxied call window**.
- DEK is fetched per-call, never cached persistently in the gateway process.
- KMS is the only long-lived secret store; gateway holds no durable key material.

### 4.2 Gateway Hardening

- **mTLS** between the API Gateway and the KMS/Vault service — prevents unauthorized internal service calls.
- **Structured log sanitization middleware** — key material, tokens, and secrets are explicitly stripped from all log output before writing; keys never touch disk or log files in any form.
- **Strict IAM roles** — the gateway service account has minimum permissions: only KMS decrypt, no KMS admin or key listing.

---

## 5. System Architecture

### 5.1 High-Level Layer Map

```
[ CLIENT & INTERFACE LAYER ]
  Users (Dev A, Dev B, Dev C)
        |
        v
  Next.js Web App (App Router, React 19)
  Zustand + Liveblocks (presence/UI real-time)
  Graph Visualizer / Task Planner
        |
        | HTTPS / SSE Stream
        v

[ API GATEWAY & PROXY LAYER ]
  Go or FastAPI high-throughput proxy
  Redis rate limiting
  mTLS <-> Cloud KMS / HashiCorp Vault (AES-256 envelope encryption)
        |
        +----------------------------------+
        |                                  |
        | BYOK Streaming Calls             | Internal Request Pipeline
        v                                  v

[ EXTERNAL LLM PROVIDERS ]         [ QUEEN AGENT & ORCHESTRATION ]
  Google Gemini API                   Task & Implementation Planning
  OpenAI (GPT-4o)                     Context Conflict Detection (proposes)
  Anthropic (Claude)                  Semantic Sub-graph Context Router
                                              |
                          +------------------+------------------+
                          |                                     |
                          v                                     v
                [ Async Task Queue ]            [ Unified Context Storage ]
                  Redis / Inngest                PostgreSQL (single instance)
                          |                        - Apache AGE (graph)
                          v                        - pgvector (vector index)
                [ Extraction & Parser Worker ]     - Relational tables
                  LLM extraction (Gemini Flash)
                  Pydantic schema validation
                  Writes graph nodes on success
                  Discards malformed extractions
```

### 5.2 Downstream Delivery

| Mode | Mechanism | Notes |
|---|---|---|
| **Mode A: Direct IDE Sync** | Read-only MCP Server Endpoint | IDE queries context, fetches task specs and sub-graphs; no write-back from IDEs |
| **Mode B: Standalone Export** | Structured Markdown blueprints | Developer pastes into standard editor or local terminal manually |

---

## 6. Data Flow & Processing Lifecycle

1. **Ingestion & Interception**: Developer makes a workspace-scoped LLM call through the proxy. Gateway buffers the full SSE stream server-side.
2. **Stream Completion Gate**: Extraction is triggered **only on clean stream close**. Interrupted/partial streams are discarded entirely (not queued); a `partial_transcript` event is logged for observability but extraction is skipped.
3. **Normalization & Extraction**: Extraction worker uses Gemini Flash with few-shot prompting to produce structured JSON. Output is validated against a Pydantic schema. Malformed extractions are rejected and logged — never written to graph.
4. **Graph Mutation**: Valid entities and relationships are written into the Apache AGE graph as a "Context Commit." The branch's monotonic **mutation version counter** is incremented atomically.
5. **Cache Invalidation**: Semantic cache entries keyed on `(workspace_id, branch_id, query_vector, mutation_version)` are invalidated when the branch version counter advances.
6. **Conflict Detection**: If two concurrent writes produce conflicting nodes (entity ID collision, dependency cycle), the system flags a **Context Conflict** and presents a diff view. A human approves or rejects the Queen Agent's proposed resolution — no silent auto-merges.

---

## 7. Storage Architecture

**Single PostgreSQL instance** hosting three co-located data concerns:

| Component | Engine | Purpose |
|---|---|---|
| **Relational Store** | PostgreSQL tables | Users, orgs, workspace permissions, encrypted BYOK vault references, billing metadata |
| **Knowledge Graph** | Apache AGE (Cypher on PostgreSQL) | Entities (functions, modules, decisions) and directed relations (dependencies, ownership, conflicts) |
| **Vector Index** | pgvector extension | Semantic retrieval of entity embeddings, k-NN lookup for GraphRAG entry points |

> **Why single instance?** Eliminates the dual-write consistency problem between graph and vector stores. Apache AGE and pgvector coexist in the same PostgreSQL transaction boundary.

### Schema-per-tenant multi-tenancy
- Each organization gets its own PostgreSQL schema (`org_<id>.*`).
- Moderate isolation, clean data export, simpler GDPR/deletion handling.
- Schema migrations are run per-tenant via a migration orchestrator (e.g., Flyway or a custom migration runner).

---

## 8. Branching Model

**Virtual branching via node metadata** — no physical graph copies.

- Every node carries a `branch_id` tag.
- The full graph lives in a single namespace; queries filter by `branch_id`.
- Merge = write new nodes tagged with the target `branch_id` + conflict resolution step.

### Performance strategy
A **branch-aware query service** sits in front of the graph DB:
- Injects `branch_id` filter on every query automatically.
- Caches per-branch sub-graphs in **Redis** (keyed on `(workspace_id, branch_id, mutation_version)`).
- Stale/archived branches have their cache entries evicted and their nodes excluded from hot indexes via a composite index on `(branch_id, node_type, created_at)`.

---

## 9. Unified Context Storage: Hybrid GraphRAG & Semantic Caching

```
[ Incoming Query / Task Request ]
        |
        v
[ Generate Query Vector ]
        |
        v
[ Semantic Cache Lookup ]
  Key: (workspace_id, branch_id, query_vector, branch_mutation_version)
  Hit (cosine similarity >= 0.92 AND version matches) -> Return cached (0 tokens, <20ms)
        | Cache Miss or Version Mismatch
        v
[ Hybrid GraphRAG Retrieval ]
  - Vector Search: k-NN entry nodes via pgvector
  - Graph Traversal: 1-2 hop relation exploration via Apache AGE
        |
        v
[ Pruning & Compression ]
  - Cross-Encoder Re-ranking (Top 3-5 candidates)
  - Prompt Pruning (LLMLingua / Minified Triplets)
        |
        v
[ Queen Agent Processing & Context Injection ]
        |
        v
[ Update Semantic Cache & Graph ]
  Cache keyed with current branch_mutation_version
```

### Token & Storage Optimization
- **Semantic Caching**: Redis stores `(query_vector, branch_mutation_version) -> response`. Invalidated when branch version counter increments.
- **Tuple/Triplet Representation**: Context compressed into `(AuthService)-[depends_on]->(PostgreSQL)` form.
- **Recency & Decay Scoring**: `S = S_vector * e^(-lambda * delta_t)` — exponential decay deprecates stale nodes.
- **Node Lifecycle**: `active -> decayed -> archived`. Archived nodes are moved to a cold JSONB blob in PostgreSQL, excluded from live queries, restoreable on demand. **Nodes are never hard-deleted** (audit trail preserved).
- **Dynamic Token Budgeting**: <= 150 tokens for lookups, < 600 tokens for complex synthesis.

---

## 10. Real-Time Collaboration Model

| Concern | Tool | Consistency Model |
|---|---|---|
| **Presence, cursors, live editing indicators** | Liveblocks | Real-time (WebSocket) |
| **Knowledge graph mutations** | Async extraction pipeline | Eventually consistent |

> The UI shows graph state from the last completed extraction. There is a visible staleness window between when a conversation completes and when the extraction worker writes the new nodes. This is **acceptable and explicit** — no attempt to make graph writes real-time synchronized in v1.

---

## 11. Access Control

### Authentication
- **GitHub OAuth** for v1 — aligns with developer user base and the "Git for AI context" positioning.
- Use a managed auth provider (Clerk or Supabase Auth) wrapping GitHub OAuth — no custom auth code.

### Intra-Workspace RBAC

| Role | Permissions |
|---|---|
| **Admin** | Full graph write/delete, API key management, billing, user management, approve conflict resolutions |
| **Contributor** | Read + write new context nodes, promote personal calls to shared context; **cannot delete confirmed nodes** |
| **Viewer** | Read-only; can consume exported specs and Markdown blueprints; cannot mutate graph |

---

## 12. Observability Stack

| Concern | Tool | Audience |
|---|---|---|
| LLM tracing (prompt/response/latency/cost per call) | **Langfuse** | Internal ops + workspace Admins |
| Gateway & worker structured logs | **JSON logs** (GCP Logging / CloudWatch) | Internal ops |
| Infrastructure metrics (latency, throughput, error rates) | **Prometheus + Grafana** | Internal ops |

> Cost visibility is **deferred to v2** — no per-workspace spend dashboard or hard budget caps in v1. Known risk: a runaway Queen Agent loop could burn significant BYOK budget. Accepted as v1 debt.

---

## 13. Tech Stack (Confirmed)

| Layer | Technology |
|---|---|
| **Frontend** | Next.js (App Router, React 19), TypeScript, Tailwind CSS, shadcn/ui, Zustand, Liveblocks |
| **API Gateway / Proxy** | Go or FastAPI (Python) — high-throughput SSE streaming, Redis rate limiting |
| **Worker & Orchestration** | Redis / Inngest queue, Celery, LangGraph (Queen Agent state machine), Gemini Flash (extraction) |
| **Storage** | PostgreSQL + Apache AGE + pgvector (single instance, schema-per-tenant) |
| **Auth** | Clerk or Supabase Auth (GitHub OAuth) |
| **Security** | HashiCorp Vault / Cloud KMS (DEK/KEK envelope encryption), mTLS, log sanitization middleware |
| **Observability** | Langfuse (LLM traces), Prometheus + Grafana (infra), structured JSON logs |

---

## 14. Known Risks & Deferred Items (v2 Backlog)

| Item | Risk Level | Notes |
|---|---|---|
| **Cost runaway / spend visibility** | HIGH | No hard budget caps in v1; BYOK burn is customer-visible |
| **Git integration (ingestion phase 2)** | MEDIUM | Required to close the cold-start coverage gap beyond manual import |
| **Browser extension (ingestion phase 3)** | MEDIUM | Broadest capture scope but highest implementation complexity |
| **Schema migration orchestration at scale** | MEDIUM | Schema-per-tenant migrations become expensive as org count grows |
| **MCP write-back** | LOW | v1 is read-only; write-back from IDEs deferred |
| **HSM for key decryption** | LOW | mTLS + log sanitization accepted for v1; HSM is a v2 hardening option |
| **Fine-grained ABAC / node-level locking** | LOW | Three-role RBAC sufficient for v1 |
