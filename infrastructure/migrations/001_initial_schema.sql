-- 001_initial_schema.sql
-- Initial relational and vector schema for Collaborative AI Context Synchronization Platform

-- Enable core extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS age;

LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Workspaces
CREATE TABLE IF NOT EXISTS workspaces (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Branches
CREATE TABLE IF NOT EXISTS branches (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name             VARCHAR(100) NOT NULL DEFAULT 'main',
    mutation_version BIGINT NOT NULL DEFAULT 1,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, name)
);

CREATE INDEX IF NOT EXISTS idx_branches_workspace_id ON branches(workspace_id);

-- Envelope-encrypted provider keys
-- Key rotation MUST regenerate both encrypted_key AND key_nonce atomically
CREATE TABLE IF NOT EXISTS workspace_provider_keys (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id  UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    provider      VARCHAR(50) NOT NULL,   -- 'openai' | 'anthropic' | 'gemini'
    encrypted_dek BYTEA NOT NULL,         -- DEK wrapped by Cloud KMS / Vault KEK
    encrypted_key BYTEA NOT NULL,         -- Provider key encrypted by DEK (AES-256-GCM)
    key_nonce     BYTEA NOT NULL,         -- 12-byte GCM nonce; regenerated on rotation
    key_hash      VARCHAR(64) NOT NULL,   -- SHA-256 fingerprint
    created_by    UUID NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(workspace_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_provider_keys_workspace ON workspace_provider_keys(workspace_id);

-- Transcripts: raw_payload is stripped after confirmed extraction
-- Stored encrypted (same DEK as provider keys) to protect PII / trade secrets
CREATE TABLE IF NOT EXISTS transcripts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    branch_id        UUID NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    actor_id         UUID NOT NULL,
    provider         VARCHAR(50) NOT NULL,
    model            VARCHAR(100) NOT NULL,
    stream_status    VARCHAR(20) NOT NULL,  -- 'completed' | 'interrupted' | 'error'
    prompt_tokens    INT DEFAULT 0,
    completion_tokens INT DEFAULT 0,
    raw_payload      BYTEA,                 -- AES-256-GCM encrypted JSONB; NULLed post-extraction
    extraction_done  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transcripts_branch ON transcripts(branch_id);
CREATE INDEX IF NOT EXISTS idx_transcripts_workspace ON transcripts(workspace_id);

-- Context commits audit table
CREATE TABLE IF NOT EXISTS context_commits (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    branch_id        UUID NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    transcript_id    UUID NOT NULL REFERENCES transcripts(id) ON DELETE CASCADE,
    mutation_version BIGINT NOT NULL,        -- post-commit version
    nodes_written    INT NOT NULL DEFAULT 0,
    edges_written    INT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_context_commits_branch ON context_commits(branch_id);
CREATE INDEX IF NOT EXISTS idx_context_commits_version ON context_commits(workspace_id, branch_id, mutation_version);

-- Entity embeddings: text-embedding-3-small (1536 dims)
CREATE TABLE IF NOT EXISTS entity_embeddings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id  UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    branch_id     UUID NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    graph_node_id VARCHAR(255) NOT NULL,
    node_type     VARCHAR(50) NOT NULL,
    name          VARCHAR(255) NOT NULL,
    description   TEXT NOT NULL,
    embedding     vector(1536) NOT NULL,   -- text-embedding-3-small ONLY
    mutation_version BIGINT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(branch_id, graph_node_id)
);

CREATE INDEX IF NOT EXISTS idx_ee_knn ON entity_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_ee_lookup ON entity_embeddings(workspace_id, branch_id, mutation_version);
