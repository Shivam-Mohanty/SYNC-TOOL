import pytest
from services.worker.src.storage.graph import (
    safe_slug,
    safe_label,
    safe_name,
    safe_summary,
    safe_org_id,
    commit_to_graph_and_vector
)
from services.worker.src.models import ContextExtractionResult, ExtractedNode, NodeType, ExtractedEdge, EdgeType

# Adversarial SQL, Cypher, Shell, and Metacharacter Payloads
ADVERSARIAL_PAYLOADS = [
    # SQL Injections
    "'; DROP TABLE workspaces; --",
    "' OR '1'='1",
    "admin' --",
    "1; SELECT pg_sleep(5);",
    "1' UNION SELECT username, password FROM users --",
    "DROP TABLE branches CASCADE;",
    
    # Cypher Injections & AGE Escapes
    "$$ BREAKOUT $$",
    "$$ MATCH (n) DETACH DELETE n; $$",
    "node' MATCH (m) RETURN m --",
    "MERGE (a)-[r:ADMIN]->(b)",
    "n}) RETURN n; //",
    "$$); DROP EXTENSION age; --",
    "test $$; CREATE (n:Evil {name:'pwned'}); $$",

    # Delimiters and Syntax Metacharacters
    'node" OR "1"="1',
    "node`test`",
    "node;--",
    "node/*comment*/",
    "node\\x00nullbyte",
    "node$injection",
    "node{key: 'val'}",
    "node[0]",
    "node<script>alert(1)</script>",

    # Whitespace, Trailing, and Uppercase Metacharacters
    "node id with spaces",
    "   leading_space",
    "UPPERCASE_NODE_ID",
    "-leading-hyphen",
    "_leading_underscore",
    "",  # Empty
    "a" * 150,  # Exceeds max length (128)
]

def test_safe_slug_rejects_adversarial_metacharacters():
    """
    Fuzz node.id and edge slugs with SQL/Cypher metacharacters.
    Must raise ValueError for all adversarial inputs.
    """
    for payload in ADVERSARIAL_PAYLOADS:
        with pytest.raises(ValueError, match="Unsafe node or edge slug rejected"):
            safe_slug(payload)

def test_safe_slug_accepts_valid_slugs():
    """Valid slugs matching ^[a-z0-9][a-z0-9-_]{0,127}$ must pass."""
    valid_slugs = [
        "auth-service",
        "redis-cache",
        "api-gw-proxy",
        "postgres-16-age",
        "c39a8adc-f8af-436f-88a2-ca713f9f5636",
        "node1",
        "decision_pkce_auth",
        "t",
        "a" * 128
    ]
    for slug in valid_slugs:
        assert safe_slug(slug) == slug

def test_safe_name_rejects_cypher_and_sql_metacharacters():
    """
    Fuzz node.name with SQL and Cypher injection payloads.
    Must raise ValueError when metacharacters like ', \", $, ;, \\, comment markers are present.
    """
    adversarial_names = [
        "Component'; DROP TABLE workspaces; --",
        "Component $$ BREAKOUT $$",
        "Component' OR 1=1 --",
        "Component; SELECT pg_sleep(5);",
        "Component /* inline comment */",
        "Component -- sql comment",
        "Component\\x00nullbyte",
        'Component" double quotes',
        "Component $variable",
        "Component \\ backslash",
        "",  # Empty
        "   ",  # Whitespace only
        "A" * 300,  # Exceeds 255 chars
    ]
    for bad_name in adversarial_names:
        with pytest.raises(ValueError, match="Unsafe node name rejected"):
            safe_name(bad_name)

def test_safe_name_accepts_legitimate_names():
    """Accepts valid human-readable architecture entity names."""
    valid_names = [
        "Authentication Service",
        "API Gateway (Go)",
        "PostgreSQL 16 + Apache AGE",
        "OAuth 2.0 PKCE Flow",
        "Redis Streams: completed queue",
        "Token Budget: 6,000 Tokens",
        "Component #1",
        "Client @ Downstream",
        "Worker / Consumer",
    ]
    for name in valid_names:
        assert safe_name(name) == name.strip()

def test_safe_label_rejects_unauthorized_labels():
    """Validates that only allowed vertex and edge labels are accepted."""
    unauthorized = [
        "User", "Admin", "DROP", "WORKSPACE", "workspace",
        "TABLE", "CYPHER", "Edge", "Node", "evil_label",
        "Component; DROP TABLE", "DEPENDS_ON$$"
    ]
    for label in unauthorized:
        with pytest.raises(ValueError, match="Unknown or unauthorized graph label"):
            safe_label(label)

def test_safe_label_accepts_allowed_labels():
    allowed = [
        "Component", "Decision", "Constraint", "Task",
        "DEPENDS_ON", "MODIFIES", "CONSTRAINED_BY", "DECIDED_IN", "BLOCKS"
    ]
    for label in allowed:
        assert safe_label(label) == label

def test_safe_summary_escapes_and_rejects_breakout():
    """Ensures safe_summary escapes quotes and rejects dollar-quote escapes."""
    with pytest.raises(ValueError, match="dollar-quote breakout"):
        safe_summary("Breaks $$ Cypher $$ syntax")

    with pytest.raises(ValueError, match="null byte"):
        safe_summary("Null byte \x00 attack")

    # Clean quotes escaping
    raw = "User's session isn't invalidated"
    sanitized = safe_summary(raw)
    assert "\\'" in sanitized

def test_safe_org_id_rejects_injection():
    """Validates org_id for per-tenant graph creation."""
    with pytest.raises(ValueError, match="Unsafe org_id rejected"):
        safe_org_id("org; DROP GRAPH;")
    with pytest.raises(ValueError, match="Unsafe org_id rejected"):
        safe_org_id("org' OR '1'='1")
    with pytest.raises(ValueError, match="Unsafe org_id rejected"):
        safe_org_id("org$$breakout")

    assert safe_org_id("org-acme-corp") == "org-acme-corp"
    assert safe_org_id("c39a8adc") == "c39a8adc"

@pytest.mark.asyncio
async def test_commit_to_graph_rejects_adversarial_extraction():
    """
    Verifies that commit_to_graph_and_vector halts immediately with ValueError
    when given adversarial nodes, without touching the database.
    """
    adversarial_extraction = ContextExtractionResult(
        has_meaningful_context=True,
        nodes=[
            ExtractedNode(
                id="safe-node",
                name="Safe Node",
                node_type=NodeType.COMPONENT,
                summary="Normal summary",
                confidence=1.0
            ),
            ExtractedNode(
                id="safe-node-2",
                name="Adversarial'; DROP TABLE users; --",
                node_type=NodeType.COMPONENT,
                summary="Malicious injection attempt",
                confidence=1.0
            )
        ],
        edges=[]
    )

    with pytest.raises(ValueError, match="Unsafe node name rejected"):
        await commit_to_graph_and_vector(
            workspace_id="ws-1",
            branch_id="main",
            transcript_id="tx-1",
            extraction=adversarial_extraction
        )
