import pytest
from pydantic import ValidationError
from services.worker.src.models import (
    NodeType,
    EdgeType,
    ExtractedNode,
    ExtractedEdge,
    ContextExtractionResult,
)

def test_valid_extracted_node():
    node = ExtractedNode(
        id="api-gateway",
        name="API Gateway",
        node_type=NodeType.COMPONENT,
        summary="High throughput Go proxy with SSE tee streaming.",
        confidence=0.95
    )
    assert node.id == "api-gateway"
    assert node.node_type == NodeType.COMPONENT
    assert node.confidence == 0.95

def test_node_id_slug_normalization():
    # Uppercase and space normalized to valid slug
    node = ExtractedNode(
        id="API Gateway Service!",
        name="API Gateway",
        node_type=NodeType.COMPONENT,
        summary="Proxy handler.",
        confidence=0.9
    )
    assert node.id == "api-gateway-service"

def test_confidence_validation():
    with pytest.raises(ValidationError):
        ExtractedNode(
            id="test-node",
            name="Test",
            node_type=NodeType.TASK,
            summary="Invalid confidence",
            confidence=1.5  # Exceeds 1.0
        )

def test_valid_extracted_edge():
    edge = ExtractedEdge(
        source_id="gateway-service",
        target_id="redis-queue",
        edge_type=EdgeType.DEPENDS_ON
    )
    assert edge.source_id == "gateway-service"
    assert edge.target_id == "redis-queue"
    assert edge.edge_type == EdgeType.DEPENDS_ON

def test_context_extraction_result_json_roundtrip():
    payload = """
    {
        "has_meaningful_context": true,
        "nodes": [
            {
                "id": "vault-kms",
                "name": "HashiCorp Vault",
                "node_type": "Component",
                "summary": "Master key encryption engine.",
                "confidence": 0.98
            }
        ],
        "edges": []
    }
    """
    res = ContextExtractionResult.model_validate_json(payload)
    assert res.has_meaningful_context is True
    assert len(res.nodes) == 1
    assert res.nodes[0].name == "HashiCorp Vault"
    assert res.nodes[0].node_type == NodeType.COMPONENT

def test_empty_extraction_result():
    res = ContextExtractionResult(has_meaningful_context=False)
    assert res.has_meaningful_context is False
    assert len(res.nodes) == 0
    assert len(res.edges) == 0
