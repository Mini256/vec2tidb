import uuid


def generate_unique_name(prefix: str = "test_vec2tidb") -> str:
    """Generate a unique name with the given prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"