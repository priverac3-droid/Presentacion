from unittest.mock import MagicMock, patch

import pytest

from src import index
from src.index import lambda_handler


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    for variable_name in [
        "DAILY_DATASYNC_TASK_ARN",
        "HISTORICAL_DATASYNC_TASK_ARN",
        "DAILY_INCLUDE_PATTERNS",
        "HISTORICAL_INCLUDE_PATTERNS",
        "DAILY_EXCLUDE_PATTERNS",
        "HISTORICAL_EXCLUDE_PATTERNS",
        "DAILY_OVERRIDE_OPTIONS_JSON",
        "HISTORICAL_OVERRIDE_OPTIONS_JSON",
        "DEFAULT_INCLUDE_PATTERNS",
        "DEFAULT_EXCLUDE_PATTERNS",
        "ORCHESTRATOR_SECRET_ARN",
    ]:
        monkeypatch.delenv(variable_name, raising=False)

    index._load_secret_configuration.cache_clear()
    yield
    index._load_secret_configuration.cache_clear()


def _build_datasync_mock():
    mock_datasync = MagicMock()

    class MockInvalidRequestException(Exception):
        pass

    mock_datasync.exceptions.InvalidRequestException = MockInvalidRequestException
    mock_datasync.start_task_execution.return_value = {
        "TaskExecutionArn": "arn:aws:datasync:us-east-1:123456789012:execution/exec-001"
    }
    return mock_datasync, MockInvalidRequestException


def test_helper_functions_validate_events_and_json():
    assert index._normalize_event(None) == {}
    assert index._normalize_event("   ") == {}
    assert index._normalize_event("historical") == {"executionMode": "historical"}
    assert index._normalize_event('{"executionMode":"daily"}') == {"executionMode": "daily"}

    with pytest.raises(ValueError, match="objeto JSON"):
        index._normalize_event('["not", "a", "dict"]')

    with pytest.raises(ValueError, match="objeto JSON"):
        index._normalize_event(123)

    assert index._parse_json_object("", "TEST_SOURCE") == {}

    with pytest.raises(ValueError, match="no es JSON válido"):
        index._parse_json_object("{", "TEST_SOURCE")

    with pytest.raises(ValueError, match="debe ser un objeto JSON"):
        index._parse_json_object("[]", "TEST_SOURCE")


def test_helper_functions_validate_secret_regions_and_mode_config():
    assert index._extract_region_from_secret_arn("invalid") is None
    assert index._extract_region_from_secret_arn(
        "arn:aws:ssm:us-east-1:123456789012:parameter/foo"
    ) is None
    assert index._extract_region_from_secret_arn(
        "arn:aws:secretsmanager:us-west-2:123456789012:secret:ach/config"
    ) == "us-west-2"

    with pytest.raises(ValueError, match="debe ser un objeto"):
        index._get_mode_secret({"daily": "bad-value"}, "daily")


def test_helper_functions_validate_filters_and_overrides():
    assert index._normalize_filter_patterns("Listado| /TRTP-IN |Listado") == "/Listado|/TRTP-IN"
    assert index._normalize_filter_patterns([]) is None

    with pytest.raises(ValueError, match="cadena separada"):
        index._normalize_filter_patterns(10)

    assert index._normalize_override_options("") == {}
    assert index._normalize_override_options(
        {"BytesPerSecond": 1, "LogLevel": "", "Uid": None}
    ) == {"BytesPerSecond": 1}

    with pytest.raises(ValueError, match="objeto JSON"):
        index._normalize_override_options([])


def test_build_start_request_supports_excludes_and_rejects_empty_explicit_task(monkeypatch):
    monkeypatch.setenv(
        "HISTORICAL_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/historical-001",
    )

    execution_mode, request = index._build_start_request(
        {
            "executionMode": "historical",
            "excludePatterns": ["reportes", "/TRTP-OUT"],
        }
    )

    assert execution_mode == "historical"
    assert request["Excludes"] == [{
        "FilterType": "SIMPLE_PATTERN",
        "Value": "/reportes|/TRTP-OUT",
    }]

    with pytest.raises(ValueError, match="Falta taskArn"):
        index._build_start_request({"executionMode": "custom", "taskArn": ""})


@patch("boto3.client")
def test_lambda_daily_success(mock_boto_client, monkeypatch):
    mock_datasync, _ = _build_datasync_mock()
    mock_boto_client.return_value = mock_datasync
    monkeypatch.setenv(
        "DAILY_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/daily-001",
    )

    response = lambda_handler({}, {})

    assert response["statusCode"] == 200
    assert response["body"]["executionMode"] == "daily"
    mock_datasync.start_task_execution.assert_called_once_with(
        TaskArn="arn:aws:datasync:us-east-1:123456789012:task/daily-001",
        OverrideOptions={
            "BytesPerSecond": 20971520,
            "LogLevel": "BASIC",
            "OverwriteMode": "ALWAYS",
            "PreserveDeletedFiles": "PRESERVE",
            "TaskQueueing": "ENABLED",
            "TransferMode": "CHANGED",
            "VerifyMode": "POINT_IN_TIME_CONSISTENT",
        },
    )


def test_lambda_missing_task_configuration():
    response = lambda_handler({}, {})

    assert response["statusCode"] == 400
    assert "Falta configurar el ARN de DataSync para el modo daily" in response["body"]["message"]


@patch("boto3.client")
def test_lambda_historical_dry_run_with_filters(mock_boto_client, monkeypatch):
    mock_datasync, _ = _build_datasync_mock()
    mock_boto_client.return_value = mock_datasync
    monkeypatch.setenv(
        "HISTORICAL_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/historical-001",
    )

    response = lambda_handler(
        {
            "dryRun": True,
            "executionMode": "historical",
            "includePatterns": ["Listado", "/TRTP-IN"],
            "overrideOptions": {"BytesPerSecond": 7340032},
        },
        {},
    )

    assert response["statusCode"] == 200
    assert response["body"]["startTaskExecutionRequest"]["Includes"] == [{
        "FilterType": "SIMPLE_PATTERN",
        "Value": "/Listado|/TRTP-IN",
    }]
    assert response["body"]["startTaskExecutionRequest"]["OverrideOptions"]["TransferMode"] == "ALL"
    assert response["body"]["startTaskExecutionRequest"]["OverrideOptions"]["BytesPerSecond"] == 7340032
    mock_datasync.start_task_execution.assert_not_called()


@patch("boto3.client")
def test_lambda_invalid_request(mock_boto_client, monkeypatch):
    mock_datasync, invalid_request_exception = _build_datasync_mock()
    mock_datasync.start_task_execution.side_effect = invalid_request_exception("La tarea ya está corriendo")
    mock_boto_client.return_value = mock_datasync
    monkeypatch.setenv(
        "DAILY_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/daily-001",
    )

    response = lambda_handler({}, {})

    assert response["statusCode"] == 400
    assert "La tarea ya está corriendo" in response["body"]["message"]


@patch("boto3.client")
def test_lambda_generic_exception(mock_boto_client, monkeypatch):
    mock_datasync, _ = _build_datasync_mock()
    mock_datasync.start_task_execution.side_effect = Exception("Error de conexión fatal con AWS")
    mock_boto_client.return_value = mock_datasync
    monkeypatch.setenv(
        "DAILY_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/daily-001",
    )

    with pytest.raises(Exception) as excinfo:
        lambda_handler({}, {})

    assert "Error de conexión fatal" in str(excinfo.value)


@patch("boto3.client")
def test_lambda_reads_secret_configuration(mock_boto_client, monkeypatch):
    mock_datasync, _ = _build_datasync_mock()
    mock_secrets = MagicMock()
    boto3_client_calls = []
    mock_secrets.get_secret_value.return_value = {
        "SecretString": (
            '{"historical": {"includePatterns": ["/Listado", "reportes"], '
            '"overrideOptions": {"BytesPerSecond": 15728640}}}'
        )
    }

    def client_factory(service_name, **kwargs):
        boto3_client_calls.append((service_name, kwargs))
        if service_name == "datasync":
            return mock_datasync
        if service_name == "secretsmanager":
            return mock_secrets
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory
    monkeypatch.setenv(
        "HISTORICAL_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/historical-001",
    )
    monkeypatch.setenv(
        "HISTORICAL_OVERRIDE_OPTIONS_JSON",
        '{"BytesPerSecond": 5242880}',
    )
    monkeypatch.setenv(
        "ORCHESTRATOR_SECRET_ARN",
        "arn:aws:secretsmanager:us-west-2:123456789012:secret:ach/config",
    )

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 200
    assert ("secretsmanager", {"region_name": "us-west-2"}) in boto3_client_calls
    mock_datasync.start_task_execution.assert_called_once_with(
        TaskArn="arn:aws:datasync:us-east-1:123456789012:task/historical-001",
        Includes=[{
            "FilterType": "SIMPLE_PATTERN",
            "Value": "/Listado|/reportes",
        }],
        OverrideOptions={
            "BytesPerSecond": 15728640,
            "LogLevel": "BASIC",
            "OverwriteMode": "ALWAYS",
            "PreserveDeletedFiles": "PRESERVE",
            "TaskQueueing": "ENABLED",
            "TransferMode": "ALL",
            "VerifyMode": "POINT_IN_TIME_CONSISTENT",
        },
    )


@patch("boto3.client")
def test_lambda_mode_env_override_beats_default_secret_override(mock_boto_client, monkeypatch):
    mock_datasync, _ = _build_datasync_mock()
    mock_secrets = MagicMock()
    mock_secrets.get_secret_value.return_value = {
        "SecretString": (
            '{"defaultOverrideOptions": {"BytesPerSecond": 15728640}}'
        )
    }

    def client_factory(service_name, **kwargs):
        if service_name == "datasync":
            return mock_datasync
        if service_name == "secretsmanager":
            return mock_secrets
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory
    monkeypatch.setenv(
        "HISTORICAL_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/historical-001",
    )
    monkeypatch.setenv(
        "HISTORICAL_OVERRIDE_OPTIONS_JSON",
        '{"BytesPerSecond": 5242880}',
    )
    monkeypatch.setenv(
        "ORCHESTRATOR_SECRET_ARN",
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:ach/config",
    )

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 200
    mock_datasync.start_task_execution.assert_called_once_with(
        TaskArn="arn:aws:datasync:us-east-1:123456789012:task/historical-001",
        OverrideOptions={
            "BytesPerSecond": 5242880,
            "LogLevel": "BASIC",
            "OverwriteMode": "ALWAYS",
            "PreserveDeletedFiles": "PRESERVE",
            "TaskQueueing": "ENABLED",
            "TransferMode": "ALL",
            "VerifyMode": "POINT_IN_TIME_CONSISTENT",
        },
    )


def test_lambda_rejects_invalid_execution_mode():
    response = lambda_handler({"executionMode": "weekly"}, {})

    assert response["statusCode"] == 400
    assert "executionMode debe ser 'daily' o 'historical'" in response["body"]["message"]


def test_lambda_rejects_invalid_override_option(monkeypatch):
    monkeypatch.setenv(
        "DAILY_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/daily-001",
    )

    response = lambda_handler(
        {"overrideOptions": {"UnsupportedOption": True}},
        {},
    )

    assert response["statusCode"] == 400
    assert "UnsupportedOption" in response["body"]["message"]