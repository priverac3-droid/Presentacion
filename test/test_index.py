import io
from datetime import datetime, timezone
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
        "SOURCE_BUCKET_NAME",
        "SOURCE_PREFIX",
        "DESTINATION_BUCKET_NAME",
        "DESTINATION_PREFIX",
        "DATASYNC_ACCESS_ROLE_ARN",
        "MANIFEST_SUBDIRECTORY",
        "TRANSFER_LAST_MODIFIED_BEFORE",
        "DELETE_TRANSFERRED_SOURCE_OBJECTS",
        "LOG_LEVEL",
    ]:
        monkeypatch.delenv(variable_name, raising=False)

    index._load_secret_configuration.cache_clear()
    yield
    index._load_secret_configuration.cache_clear()


def _set_base_transfer_env(monkeypatch, execution_mode="historical"):
    monkeypatch.setenv("SOURCE_BUCKET_NAME", "dev3-integrations-t1-batch-outputfiledirectory-507781971948")
    monkeypatch.setenv("SOURCE_PREFIX", "replication/ACH/")
    monkeypatch.setenv("DESTINATION_BUCKET_NAME", "b1-useast1-dev3-coreb-backuptransversal-507781971948")
    monkeypatch.setenv("DESTINATION_PREFIX", "ACH/")
    monkeypatch.setenv("DATASYNC_ACCESS_ROLE_ARN", "arn:aws:iam::123456789012:role/datasync-role")
    monkeypatch.setenv("MANIFEST_SUBDIRECTORY", "_datasync/manifests/")
    monkeypatch.setenv("TRANSFER_LAST_MODIFIED_BEFORE", "2026-02-01T00:00:00Z")
    monkeypatch.setenv("DELETE_TRANSFERRED_SOURCE_OBJECTS", "true")

    if execution_mode == "historical":
        monkeypatch.setenv(
            "HISTORICAL_DATASYNC_TASK_ARN",
            "arn:aws:datasync:us-east-1:123456789012:task/historical-001",
        )
    else:
        monkeypatch.setenv(
            "DAILY_DATASYNC_TASK_ARN",
            "arn:aws:datasync:us-east-1:123456789012:task/daily-001",
        )


def _build_datasync_mock():
    mock_datasync = MagicMock()

    class MockInvalidRequestException(Exception):
        pass

    mock_datasync.exceptions.InvalidRequestException = MockInvalidRequestException
    mock_datasync.start_task_execution.return_value = {
        "TaskExecutionArn": "arn:aws:datasync:us-east-1:123456789012:task/task-001/execution/exec-001"
    }
    return mock_datasync, MockInvalidRequestException


def _build_s3_mock(object_list=None, manifest_content=None):
    mock_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": object_list or []}]
    mock_s3.get_paginator.return_value = paginator
    mock_s3.delete_objects.return_value = {}

    if manifest_content is not None:
        mock_s3.get_object.return_value = {"Body": io.BytesIO(manifest_content.encode("utf-8"))}

    return mock_s3


def test_helper_functions_validate_core_parsers():
    assert index._normalize_event(None) == {}
    assert index._normalize_event("   ") == {}
    assert index._normalize_event("historical") == {"executionMode": "historical"}
    assert index._normalize_event('{"executionMode":"daily"}') == {"executionMode": "daily"}
    assert index._parse_json_object("", "TEST_SOURCE") == {}
    assert index._extract_region_from_secret_arn(
        "arn:aws:secretsmanager:us-west-2:123456789012:secret:ach/config"
    ) == "us-west-2"
    assert index._extract_region_from_secret_arn("invalid") is None
    assert index._normalize_s3_prefix("/ACH") == "ACH/"
    assert index._normalize_s3_prefix("ACH/") == "ACH/"
    assert index._parse_bool("true") is True
    assert index._parse_bool("false") is False
    assert index._parse_bool(True) is True
    assert index._extract_bucket_name_from_arn("arn:aws:s3:::example-bucket") == "example-bucket"
    assert index._parse_iso_datetime("2026-02-01T00:00:00Z") == datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert index._parse_iso_datetime("") is None

    with pytest.raises(ValueError, match="objeto JSON"):
        index._normalize_event(123)

    with pytest.raises(ValueError, match="no es JSON válido"):
        index._parse_json_object("{", "TEST_SOURCE")

    with pytest.raises(ValueError, match="debe ser un objeto JSON"):
        index._parse_json_object("[]", "TEST_SOURCE")

    with pytest.raises(ValueError, match="booleano"):
        index._parse_bool("talvez")

    with pytest.raises(ValueError, match="S3BucketArn no puede estar vacío"):
        index._extract_bucket_name_from_arn("")


def test_helper_functions_validate_filters_and_matching():
    assert index._normalize_filter_patterns("Listado| /TRTP-IN |Listado") == "/Listado|/TRTP-IN"
    assert index._normalize_filter_patterns([]) is None
    assert index._split_filter_patterns("/Listado|/reportes") == ["/Listado", "/reportes"]
    assert index._object_is_selected("Listado/file.txt", "/Listado", None) is True
    assert index._object_is_selected("Listado/file.txt", "/Listado/*.txt", None) is True
    assert index._object_is_selected("TRTP-OUT/file.txt", "/Listado", None) is False
    assert index._object_is_selected("reportes/file.txt", None, "/reportes") is False
    assert index._normalize_override_options(
        {"BytesPerSecond": 1, "LogLevel": "", "Uid": None}
    ) == {"BytesPerSecond": 1}

    with pytest.raises(ValueError, match="cadena separada"):
        index._normalize_filter_patterns(10)

    with pytest.raises(ValueError, match="objeto JSON"):
        index._normalize_override_options([])


@patch("boto3.client")
def test_lambda_historical_dry_run_uses_cutoff_and_manifest_filtering(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    source_objects = [
        {
            "Key": "replication/ACH/Listado/file-old.txt",
            "LastModified": datetime(2026, 1, 15, tzinfo=timezone.utc),
            "Size": 100,
        },
        {
            "Key": "replication/ACH/Listado/file-feb.txt",
            "LastModified": datetime(2026, 2, 10, tzinfo=timezone.utc),
            "Size": 100,
        },
        {
            "Key": "replication/ACH/reportes/",
            "LastModified": datetime(2026, 1, 5, tzinfo=timezone.utc),
            "Size": 0,
        },
    ]
    mock_s3 = _build_s3_mock(object_list=source_objects)
    mock_boto_client.return_value = mock_s3

    response = lambda_handler(
        {
            "dryRun": True,
            "executionMode": "historical",
            "includePatterns": ["Listado"],
        },
        {},
    )

    assert response["statusCode"] == 200
    assert response["body"]["eligibleObjectCount"] == 1
    assert response["body"]["destinationPrefix"] == "ACH/"
    assert response["body"]["sampleObjects"] == ["Listado/file-old.txt"]
    mock_s3.put_object.assert_not_called()


@patch("boto3.client")
def test_lambda_historical_success_uploads_manifest_and_starts_datasync(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    source_objects = [
        {
            "Key": "replication/ACH/Listado/file-old.txt",
            "LastModified": datetime(2026, 1, 15, tzinfo=timezone.utc),
            "Size": 100,
        },
        {
            "Key": "replication/ACH/TRTP-IN/file-old.txt",
            "LastModified": datetime(2026, 1, 10, tzinfo=timezone.utc),
            "Size": 100,
        },
    ]
    mock_s3 = _build_s3_mock(object_list=source_objects)
    mock_datasync, _ = _build_datasync_mock()

    def client_factory(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3
        if service_name == "datasync":
            return mock_datasync
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 200
    assert response["body"]["eligibleObjectCount"] == 2
    uploaded_manifest = mock_s3.put_object.call_args.kwargs
    assert uploaded_manifest["Bucket"] == "b1-useast1-dev3-coreb-backuptransversal-507781971948"
    manifest_body = uploaded_manifest["Body"].decode("utf-8")
    assert "Listado/file-old.txt" in manifest_body
    assert "TRTP-IN/file-old.txt" in manifest_body

    mock_datasync.start_task_execution.assert_called_once()
    start_request = mock_datasync.start_task_execution.call_args.kwargs
    assert start_request["TaskArn"] == "arn:aws:datasync:us-east-1:123456789012:task/historical-001"
    assert start_request["ManifestConfig"]["Source"]["S3"]["S3BucketArn"] == (
        "arn:aws:s3:::b1-useast1-dev3-coreb-backuptransversal-507781971948"
    )
    assert start_request["OverrideOptions"]["TransferMode"] == "ALL"


@patch("boto3.client")
def test_lambda_returns_noop_when_no_eligible_objects(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    source_objects = [
        {
            "Key": "replication/ACH/Listado/file-feb.txt",
            "LastModified": datetime(2026, 2, 10, tzinfo=timezone.utc),
            "Size": 100,
        }
    ]
    mock_s3 = _build_s3_mock(object_list=source_objects)
    mock_boto_client.return_value = mock_s3

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 200
    assert response["body"]["eligibleObjectCount"] == 0
    assert "No hay objetos elegibles" in response["body"]["message"]


def test_list_eligible_source_objects_skips_empty_relative_key():
    mock_s3 = _build_s3_mock(
        object_list=[
            {
                "Key": "",
                "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "Size": 0,
            }
        ]
    )

    eligible = index._list_eligible_source_objects(
        s3_client=mock_s3,
        source_bucket_name="source",
        source_prefix="",
        transfer_last_modified_before=None,
        include_patterns=None,
        exclude_patterns=None,
    )

    assert eligible == []


@patch("boto3.client")
def test_lambda_invalid_request(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="daily")
    source_objects = [
        {
            "Key": "replication/ACH/TRTP-IN/file-old.txt",
            "LastModified": datetime(2026, 1, 10, tzinfo=timezone.utc),
            "Size": 100,
        }
    ]
    mock_s3 = _build_s3_mock(object_list=source_objects)
    mock_datasync, invalid_request_exception = _build_datasync_mock()
    mock_datasync.start_task_execution.side_effect = invalid_request_exception("La tarea ya está corriendo")

    def client_factory(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3
        if service_name == "datasync":
            return mock_datasync
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory

    response = lambda_handler({"executionMode": "daily"}, {})

    assert response["statusCode"] == 400
    assert "La tarea ya está corriendo" in response["body"]["message"]


@patch("boto3.client")
def test_lambda_cleanup_event_deletes_source_objects_and_restores_markers(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    mock_s3 = _build_s3_mock(manifest_content="Listado/file1.txt\nreportes/file2.txt\n")
    mock_datasync, _ = _build_datasync_mock()
    mock_datasync.describe_task_execution.return_value = {
        "ManifestConfig": {
            "Source": {
                "S3": {
                    "S3BucketArn": "arn:aws:s3:::b1-useast1-dev3-coreb-backuptransversal-507781971948",
                    "ManifestObjectPath": "_datasync/manifests/historical-test.csv",
                }
            }
        }
    }

    def client_factory(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3
        if service_name == "datasync":
            return mock_datasync
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory
    response = lambda_handler(
        {
            "source": "aws.datasync",
            "detail-type": "DataSync Task Execution State Change",
            "detail": {"State": "SUCCESS"},
            "resources": ["arn:aws:datasync:us-east-1:123456789012:task/task-001/execution/exec-001"],
        },
        {},
    )

    assert response["statusCode"] == 200
    assert response["body"]["deletedObjectCount"] == 2
    mock_datasync.describe_task_execution.assert_called_once()
    mock_s3.delete_objects.assert_called_once()
    deleted_keys = mock_s3.delete_objects.call_args.kwargs["Delete"]["Objects"]
    assert {"Key": "replication/ACH/Listado/file1.txt"} in deleted_keys
    assert {"Key": "replication/ACH/reportes/file2.txt"} in deleted_keys

    put_calls = mock_s3.put_object.call_args_list
    restored_keys = {call.kwargs["Key"] for call in put_calls}
    assert "replication/ACH/" in restored_keys
    assert "replication/ACH/Listado/" in restored_keys
    assert "replication/ACH/reportes/" in restored_keys


def test_lambda_cleanup_ignores_success_when_delete_disabled(monkeypatch):
    monkeypatch.setenv("DELETE_TRANSFERRED_SOURCE_OBJECTS", "false")

    response = lambda_handler(
        {
            "source": "aws.datasync",
            "detail-type": "DataSync Task Execution State Change",
            "detail": {"State": "SUCCESS"},
            "resources": ["arn:aws:datasync:us-east-1:123456789012:task/task-001/execution/exec-001"],
        },
        {},
    )

    assert response["statusCode"] == 200
    assert "deshabilitada" in response["body"]["message"]


@patch("boto3.client")
def test_lambda_cleanup_event_requires_execution_arn(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")

    response = lambda_handler(
        {
            "source": "aws.datasync",
            "detail-type": "DataSync Task Execution State Change",
            "detail": {"State": "SUCCESS"},
            "resources": [],
        },
        {},
    )

    assert response["statusCode"] == 400
    assert "TaskExecutionArn" in response["body"]["message"]


@patch("boto3.client")
def test_lambda_cleanup_event_ignores_execution_without_manifest(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    mock_s3 = _build_s3_mock()
    mock_datasync, _ = _build_datasync_mock()
    mock_datasync.describe_task_execution.return_value = {}

    def client_factory(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3
        if service_name == "datasync":
            return mock_datasync
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory
    response = lambda_handler(
        {
            "source": "aws.datasync",
            "detail-type": "DataSync Task Execution State Change",
            "detail": {"State": "SUCCESS"},
            "resources": ["arn:aws:datasync:us-east-1:123456789012:task/task-001/execution/exec-001"],
        },
        {},
    )

    assert response["statusCode"] == 200
    assert "no uso manifest" in response["body"]["message"]


def test_delete_source_objects_batches_and_detects_errors():
    mock_s3 = MagicMock()
    mock_s3.delete_objects.return_value = {}
    manifest_entries = [f"Listado/file-{idx}.txt" for idx in range(1001)]

    deleted_count = index._delete_source_objects(
        s3_client=mock_s3,
        source_bucket_name="source",
        source_prefix="replication/ACH/",
        manifest_entries=manifest_entries,
    )

    assert deleted_count == 1001
    assert mock_s3.delete_objects.call_count == 2

    mock_s3.delete_objects.return_value = {"Errors": [{"Key": "replication/ACH/Listado/file-1.txt"}]}
    with pytest.raises(RuntimeError, match="Error eliminando objetos del origen"):
        index._delete_source_objects_batch(
            s3_client=mock_s3,
            source_bucket_name="source",
            delete_batch=[{"Key": "replication/ACH/Listado/file-1.txt"}],
        )


@patch("boto3.client")
def test_lambda_reads_secret_configuration_and_uses_secret_region(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    source_objects = [
        {
            "Key": "replication/ACH/Listado/file-old.txt",
            "LastModified": datetime(2026, 1, 15, tzinfo=timezone.utc),
            "Size": 100,
        }
    ]
    mock_s3 = _build_s3_mock(object_list=source_objects)
    mock_datasync, _ = _build_datasync_mock()
    mock_secrets = MagicMock()
    boto3_client_calls = []
    mock_secrets.get_secret_value.return_value = {
        "SecretString": (
            '{"historical": {"includePatterns": ["/Listado"], '
            '"overrideOptions": {"BytesPerSecond": 15728640}}}'
        )
    }

    def client_factory(service_name, **kwargs):
        boto3_client_calls.append((service_name, kwargs))
        if service_name == "s3":
            return mock_s3
        if service_name == "datasync":
            return mock_datasync
        if service_name == "secretsmanager":
            return mock_secrets
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory
    monkeypatch.setenv(
        "ORCHESTRATOR_SECRET_ARN",
        "arn:aws:secretsmanager:us-west-2:123456789012:secret:ach/config",
    )

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 200
    assert ("secretsmanager", {"region_name": "us-west-2"}) in boto3_client_calls
    start_request = mock_datasync.start_task_execution.call_args.kwargs
    assert start_request["OverrideOptions"]["BytesPerSecond"] == 15728640


@patch("boto3.client")
def test_lambda_mode_env_override_beats_default_secret_override(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    source_objects = [
        {
            "Key": "replication/ACH/Listado/file-old.txt",
            "LastModified": datetime(2026, 1, 15, tzinfo=timezone.utc),
            "Size": 100,
        }
    ]
    mock_s3 = _build_s3_mock(object_list=source_objects)
    mock_datasync, _ = _build_datasync_mock()
    mock_secrets = MagicMock()
    mock_secrets.get_secret_value.return_value = {
        "SecretString": '{"defaultOverrideOptions": {"BytesPerSecond": 15728640}}'
    }

    def client_factory(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3
        if service_name == "datasync":
            return mock_datasync
        if service_name == "secretsmanager":
            return mock_secrets
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory
    monkeypatch.setenv("HISTORICAL_OVERRIDE_OPTIONS_JSON", '{"BytesPerSecond": 5242880}')
    monkeypatch.setenv(
        "ORCHESTRATOR_SECRET_ARN",
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:ach/config",
    )

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 200
    assert mock_datasync.start_task_execution.call_args.kwargs["OverrideOptions"]["BytesPerSecond"] == 5242880


def test_build_transfer_request_requires_bucket_configuration(monkeypatch):
    monkeypatch.setenv(
        "HISTORICAL_DATASYNC_TASK_ARN",
        "arn:aws:datasync:us-east-1:123456789012:task/historical-001",
    )

    with pytest.raises(ValueError, match="configuracion de buckets"):
        index._build_transfer_request({"executionMode": "historical"})


def test_lambda_rejects_invalid_execution_mode():
    response = lambda_handler({"executionMode": "weekly"}, {})

    assert response["statusCode"] == 400
    assert "executionMode debe ser 'daily' o 'historical'" in response["body"]["message"]


def test_lambda_rejects_missing_task_configuration(monkeypatch):
    monkeypatch.setenv("SOURCE_BUCKET_NAME", "source")
    monkeypatch.setenv("DESTINATION_BUCKET_NAME", "dest")
    monkeypatch.setenv("DATASYNC_ACCESS_ROLE_ARN", "arn:aws:iam::123456789012:role/datasync-role")

    response = lambda_handler({"executionMode": "historical"}, {})

    assert response["statusCode"] == 400
    assert "Falta configurar el ARN de DataSync" in response["body"]["message"]


@patch("boto3.client")
def test_lambda_reraises_unexpected_exception(mock_boto_client, monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="historical")
    mock_s3 = _build_s3_mock(
        object_list=[
            {
                "Key": "replication/ACH/Listado/file-old.txt",
                "LastModified": datetime(2026, 1, 15, tzinfo=timezone.utc),
                "Size": 100,
            }
        ]
    )
    mock_s3.put_object.side_effect = RuntimeError("fallo al guardar manifest")

    def client_factory(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3
        raise ValueError(service_name)

    mock_boto_client.side_effect = client_factory

    with pytest.raises(RuntimeError, match="fallo al guardar manifest"):
        lambda_handler({"executionMode": "historical"}, {})


def test_lambda_rejects_invalid_override_option(monkeypatch):
    _set_base_transfer_env(monkeypatch, execution_mode="daily")

    response = lambda_handler(
        {"executionMode": "daily", "overrideOptions": {"UnsupportedOption": True}},
        {},
    )

    assert response["statusCode"] == 400
    assert "UnsupportedOption" in response["body"]["message"]


def test_lambda_ignores_non_success_datasync_event():
    response = lambda_handler(
        {
            "source": "aws.datasync",
            "detail-type": "DataSync Task Execution State Change",
            "detail": {"State": "ERROR"},
            "resources": ["arn:aws:datasync:us-east-1:123456789012:task/task-001/execution/exec-001"],
        },
        {},
    )

    assert response["statusCode"] == 200
    assert "ignorado" in response["body"]["message"]
