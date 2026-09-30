from pathlib import Path


def test_feature_store_yaml_uses_feast_env_vars() -> None:
    path = Path("polyhorizon/features/feature_repo/feature_store.yaml")
    contents = path.read_text()

    assert "${POSTGRES_FEAST_DB}" in contents
    assert "${POSTGRES_FEAST_USER}" in contents
    assert "${POSTGRES_FEAST_PASSWORD}" in contents
    assert "${FEAST_SCHEMA}" in contents
    assert "${FEAST_PROJECT_NAME}" in contents
    assert "${POSTGRES_FEAST_REGISTRY_DB}" in contents
    assert "${POSTGRES_FEAST_REGISTRY_USER}" in contents
    assert "${POSTGRES_FEAST_REGISTRY_PASSWORD}" in contents
    assert "${FEAST_REGISTRY_SCHEMA}" in contents
