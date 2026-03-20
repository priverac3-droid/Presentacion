import csv
import io
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from functools import lru_cache

import boto3


DATASYNC_EVENT_SOURCE = "aws.datasync"
DATASYNC_TASK_EXECUTION_DETAIL_TYPE = "DataSync Task Execution State Change"
DATASYNC_SUCCESS_STATE = "SUCCESS"

ALLOWED_OVERRIDE_OPTIONS = {
    "Atime",
    "BytesPerSecond",
    "Gid",
    "LogLevel",
    "Mtime",
    "ObjectTags",
    "OverwriteMode",
    "PosixPermissions",
    "PreserveDeletedFiles",
    "PreserveDevices",
    "SecurityDescriptorCopyFlags",
    "TaskQueueing",
    "TransferMode",
    "Uid",
    "VerifyMode",
}

DEFAULT_OVERRIDE_OPTIONS = {
    "daily": {
        "BytesPerSecond": 20971520,
        "LogLevel": "BASIC",
        "OverwriteMode": "ALWAYS",
        "PreserveDeletedFiles": "PRESERVE",
        "TaskQueueing": "ENABLED",
        "TransferMode": "CHANGED",
        "VerifyMode": "POINT_IN_TIME_CONSISTENT",
    },
    "historical": {
        "BytesPerSecond": 52428800,
        "LogLevel": "BASIC",
        "OverwriteMode": "ALWAYS",
        "PreserveDeletedFiles": "PRESERVE",
        "TaskQueueing": "ENABLED",
        "TransferMode": "ALL",
        "VerifyMode": "POINT_IN_TIME_CONSISTENT",
    },
}

MODE_ENV_MAP = {
    "daily": {
        "exclude": "DAILY_EXCLUDE_PATTERNS",
        "include": "DAILY_INCLUDE_PATTERNS",
        "override": "DAILY_OVERRIDE_OPTIONS_JSON",
        "task_arn": "DAILY_DATASYNC_TASK_ARN",
    },
    "historical": {
        "exclude": "HISTORICAL_EXCLUDE_PATTERNS",
        "include": "HISTORICAL_INCLUDE_PATTERNS",
        "override": "HISTORICAL_OVERRIDE_OPTIONS_JSON",
        "task_arn": "HISTORICAL_DATASYNC_TASK_ARN",
    },
}


def _resolve_log_level(raw_level):
    normalized_level = str(raw_level or "INFO").strip().upper()
    return logging.getLevelNamesMapping().get(normalized_level, logging.INFO)


logger = logging.getLogger()
logger.setLevel(_resolve_log_level(os.environ.get("LOG_LEVEL")))


def _normalize_event(event):
    if event is None:
        return {}

    if isinstance(event, dict):
        return event

    if isinstance(event, str):
        stripped_event = event.strip()
        if not stripped_event:
            return {}

        try:
            parsed_event = json.loads(stripped_event)
        except json.JSONDecodeError:
            return {"executionMode": stripped_event}

        if isinstance(parsed_event, dict):
            return parsed_event

    raise ValueError("El evento debe ser un objeto JSON")


def _parse_json_object(raw_value, source_name):
    if not raw_value:
        return {}

    try:
        parsed_value = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValueError(f"El valor configurado en {source_name} no es JSON válido") from error

    if not isinstance(parsed_value, dict):
        raise ValueError(f"El valor configurado en {source_name} debe ser un objeto JSON")

    return parsed_value


def _extract_region_from_secret_arn(secret_arn):
    arn_parts = secret_arn.split(":", 5)
    if len(arn_parts) < 6:
        return None

    if arn_parts[0] != "arn" or arn_parts[2] != "secretsmanager":
        return None

    return arn_parts[3] or None


def _extract_bucket_name_from_arn(bucket_arn):
    if not bucket_arn:
        raise ValueError("S3BucketArn no puede estar vacío")

    return bucket_arn.rsplit(":::", 1)[-1]


def _parse_bool(raw_value):
    if isinstance(raw_value, bool):
        return raw_value

    normalized_value = str(raw_value or "").strip().lower()
    if normalized_value in {"1", "true", "yes", "y", "si"}:
        return True
    if normalized_value in {"0", "false", "no", "n", ""}:
        return False

    raise ValueError(f"No se pudo interpretar como booleano el valor '{raw_value}'")


def _normalize_s3_prefix(prefix):
    normalized_prefix = str(prefix or "").strip().lstrip("/")
    if normalized_prefix and not normalized_prefix.endswith("/"):
        normalized_prefix = f"{normalized_prefix}/"
    return normalized_prefix


def _parse_iso_datetime(raw_datetime):
    if not raw_datetime:
        return None

    normalized_datetime = str(raw_datetime).strip()
    if not normalized_datetime:
        return None

    normalized_datetime = normalized_datetime.replace("Z", "+00:00")
    parsed_datetime = datetime.fromisoformat(normalized_datetime)
    if parsed_datetime.tzinfo is None:
        parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)

    return parsed_datetime.astimezone(timezone.utc)


@lru_cache(maxsize=1)
def _load_secret_configuration():
    secret_arn = os.environ.get("ORCHESTRATOR_SECRET_ARN", "").strip()
    if not secret_arn:
        return {}

    logger.info("Cargando configuracion adicional desde Secrets Manager")
    secrets_manager_client_kwargs = {}
    secret_region = _extract_region_from_secret_arn(secret_arn)
    if secret_region:
        secrets_manager_client_kwargs["region_name"] = secret_region

    response = boto3.client("secretsmanager", **secrets_manager_client_kwargs).get_secret_value(
        SecretId=secret_arn
    )
    secret_string = response.get("SecretString", "")
    return _parse_json_object(secret_string, "ORCHESTRATOR_SECRET_ARN")


def _get_mode_secret(secret_config, execution_mode):
    mode_secret = secret_config.get(execution_mode, {})
    if mode_secret and not isinstance(mode_secret, dict):
        raise ValueError(f"La configuracion del modo {execution_mode} en Secrets Manager debe ser un objeto")
    return mode_secret


def _first_non_empty(*values):
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _normalize_filter_patterns(patterns):
    if patterns in (None, "", []):
        return None

    if isinstance(patterns, str):
        raw_patterns = [item.strip() for item in patterns.split("|") if item.strip()]
    elif isinstance(patterns, list):
        raw_patterns = [str(item).strip() for item in patterns if str(item).strip()]
    else:
        raise ValueError("Los filtros deben ser una cadena separada por '|' o una lista")

    normalized_patterns = []
    for pattern in raw_patterns:
        normalized_patterns.append(pattern if pattern.startswith("/") else f"/{pattern}")

    unique_patterns = list(dict.fromkeys(normalized_patterns))
    return "|".join(unique_patterns) if unique_patterns else None


def _normalize_override_options(override_options):
    if override_options in (None, ""):
        return {}

    if not isinstance(override_options, dict):
        raise ValueError("overrideOptions debe ser un objeto JSON")

    unsupported_options = sorted(set(override_options) - ALLOWED_OVERRIDE_OPTIONS)
    if unsupported_options:
        unsupported_options_str = ", ".join(unsupported_options)
        raise ValueError(f"OverrideOptions contiene claves no soportadas: {unsupported_options_str}")

    return {
        key: value
        for key, value in override_options.items()
        if value not in (None, "", [])
    }


def _split_filter_patterns(pattern_string):
    if not pattern_string:
        return []
    return [pattern for pattern in pattern_string.split("|") if pattern]


def _matches_filter(candidate_path, filter_patterns):
    for filter_pattern in filter_patterns:
        normalized_pattern = filter_pattern.rstrip("/")
        if any(token in normalized_pattern for token in "*?[]"):
            if fnmatch(candidate_path, normalized_pattern):
                return True
            continue

        if candidate_path == normalized_pattern or candidate_path.startswith(f"{normalized_pattern}/"):
            return True

    return False


def _object_is_selected(relative_key, include_patterns, exclude_patterns):
    candidate_path = f"/{relative_key}"
    include_pattern_list = _split_filter_patterns(include_patterns)
    exclude_pattern_list = _split_filter_patterns(exclude_patterns)

    if include_pattern_list and not _matches_filter(candidate_path, include_pattern_list):
        return False

    if exclude_pattern_list and _matches_filter(candidate_path, exclude_pattern_list):
        return False

    return True


def _build_manifest_key(manifest_prefix, execution_mode):
    normalized_manifest_prefix = _normalize_s3_prefix(manifest_prefix)
    manifest_file_name = f"{execution_mode}-{uuid.uuid4().hex}.csv"
    return f"{normalized_manifest_prefix}{manifest_file_name}"


def _build_manifest_config(bucket_name, manifest_object_key, bucket_access_role_arn):
    return {
        "Action": "TRANSFER",
        "Format": "CSV",
        "Source": {
            "S3": {
                "BucketAccessRoleArn": bucket_access_role_arn,
                "ManifestObjectPath": manifest_object_key,
                "S3BucketArn": f"arn:aws:s3:::{bucket_name}",
            }
        },
    }


def _list_eligible_source_objects(
    s3_client,
    source_bucket_name,
    source_prefix,
    transfer_last_modified_before,
    include_patterns,
    exclude_patterns,
):
    paginator = s3_client.get_paginator("list_objects_v2")
    eligible_keys = []

    for page in paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix):
        for object_metadata in page.get("Contents", []):
            object_key = object_metadata["Key"]

            if object_key == source_prefix or object_key.endswith("/"):
                continue

            relative_key = object_key[len(source_prefix):] if source_prefix else object_key
            if not relative_key:
                continue

            last_modified = object_metadata["LastModified"].astimezone(timezone.utc)
            if transfer_last_modified_before and last_modified >= transfer_last_modified_before:
                continue

            if not _object_is_selected(relative_key, include_patterns, exclude_patterns):
                continue

            eligible_keys.append(relative_key)

    return eligible_keys


def _upload_manifest(s3_client, destination_bucket_name, manifest_object_key, manifest_entries):
    manifest_buffer = io.StringIO()
    manifest_writer = csv.writer(manifest_buffer, lineterminator="\n")
    for manifest_entry in manifest_entries:
        manifest_writer.writerow([manifest_entry])

    s3_client.put_object(
        Bucket=destination_bucket_name,
        Key=manifest_object_key,
        Body=manifest_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def _read_manifest_entries(s3_client, manifest_bucket_name, manifest_object_key):
    manifest_object = s3_client.get_object(Bucket=manifest_bucket_name, Key=manifest_object_key)
    manifest_content = manifest_object["Body"].read().decode("utf-8")
    manifest_reader = csv.reader(io.StringIO(manifest_content))
    return [row[0].strip() for row in manifest_reader if row and row[0].strip()]


def _delete_source_objects(s3_client, source_bucket_name, source_prefix, manifest_entries):
    delete_batch = []
    total_deleted = 0

    for manifest_entry in manifest_entries:
        delete_batch.append({"Key": f"{source_prefix}{manifest_entry}"})

        if len(delete_batch) == 1000:
            _delete_source_objects_batch(s3_client, source_bucket_name, delete_batch)
            total_deleted += len(delete_batch)
            delete_batch = []

    if delete_batch:
        _delete_source_objects_batch(s3_client, source_bucket_name, delete_batch)
        total_deleted += len(delete_batch)

    return total_deleted


def _delete_source_objects_batch(s3_client, source_bucket_name, delete_batch):
    delete_response = s3_client.delete_objects(
        Bucket=source_bucket_name,
        Delete={"Objects": delete_batch, "Quiet": True},
    )

    delete_errors = delete_response.get("Errors", [])
    if delete_errors:
        raise RuntimeError(f"Error eliminando objetos del origen: {delete_errors}")


def _restore_source_folder_markers(s3_client, source_bucket_name, source_prefix, manifest_entries):
    folder_markers = set()

    if source_prefix:
        folder_markers.add(source_prefix)

    for manifest_entry in manifest_entries:
        top_level_segment = manifest_entry.split("/", 1)[0].strip()
        if top_level_segment:
            folder_markers.add(f"{source_prefix}{top_level_segment}/")

    for folder_marker in sorted(folder_markers):
        s3_client.put_object(Bucket=source_bucket_name, Key=folder_marker, Body=b"")


def _build_transfer_request(event_payload):
    execution_mode = str(event_payload.get("executionMode", "historical")).strip().lower()
    if execution_mode not in MODE_ENV_MAP and "taskArn" not in event_payload:
        raise ValueError("executionMode debe ser 'daily' o 'historical', salvo que se envíe taskArn explícito")

    secret_config = _load_secret_configuration()
    mode_secret = _get_mode_secret(secret_config, execution_mode) if execution_mode in MODE_ENV_MAP else {}

    task_arn = _first_non_empty(
        event_payload.get("taskArn"),
        os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("task_arn", ""), ""),
        mode_secret.get("taskArn"),
    )
    if not task_arn:
        if execution_mode in MODE_ENV_MAP:
            raise ValueError(f"Falta configurar el ARN de DataSync para el modo {execution_mode}")
        raise ValueError("Falta taskArn para la ejecucion solicitada")

    source_bucket_name = os.environ.get("SOURCE_BUCKET_NAME", "").strip()
    destination_bucket_name = os.environ.get("DESTINATION_BUCKET_NAME", "").strip()
    datasync_access_role_arn = os.environ.get("DATASYNC_ACCESS_ROLE_ARN", "").strip()
    if not source_bucket_name or not destination_bucket_name or not datasync_access_role_arn:
        raise ValueError("Falta la configuracion de buckets o del rol de acceso para DataSync")

    include_patterns = _normalize_filter_patterns(
        _first_non_empty(
            event_payload.get("includePatterns"),
            mode_secret.get("includePatterns"),
            os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("include", ""), ""),
            secret_config.get("defaultIncludePatterns"),
            os.environ.get("DEFAULT_INCLUDE_PATTERNS", ""),
        )
    )
    exclude_patterns = _normalize_filter_patterns(
        _first_non_empty(
            event_payload.get("excludePatterns"),
            mode_secret.get("excludePatterns"),
            os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("exclude", ""), ""),
            secret_config.get("defaultExcludePatterns"),
            os.environ.get("DEFAULT_EXCLUDE_PATTERNS", ""),
        )
    )

    env_override_options = _parse_json_object(
        os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("override", ""), ""),
        MODE_ENV_MAP.get(execution_mode, {}).get("override", "override"),
    )
    override_options = DEFAULT_OVERRIDE_OPTIONS.get(execution_mode, {}).copy()
    override_options.update(_normalize_override_options(secret_config.get("defaultOverrideOptions", {})))
    override_options.update(_normalize_override_options(env_override_options))
    override_options.update(_normalize_override_options(mode_secret.get("overrideOptions", {})))
    override_options.update(_normalize_override_options(event_payload.get("overrideOptions", {})))

    transfer_last_modified_before = _parse_iso_datetime(
        _first_non_empty(
            event_payload.get("transferLastModifiedBefore"),
            mode_secret.get("transferLastModifiedBefore"),
            secret_config.get("defaultTransferLastModifiedBefore"),
            os.environ.get("TRANSFER_LAST_MODIFIED_BEFORE", ""),
        )
    )

    delete_transferred_source_objects = _parse_bool(
        _first_non_empty(
            event_payload.get("deleteTransferredSourceObjects"),
            mode_secret.get("deleteTransferredSourceObjects"),
            secret_config.get("defaultDeleteTransferredSourceObjects"),
            os.environ.get("DELETE_TRANSFERRED_SOURCE_OBJECTS", "true"),
        )
    )

    return {
        "datasync_access_role_arn": datasync_access_role_arn,
        "delete_transferred_source_objects": delete_transferred_source_objects,
        "destination_bucket_name": destination_bucket_name,
        "destination_prefix": _normalize_s3_prefix(os.environ.get("DESTINATION_PREFIX", "")),
        "execution_mode": execution_mode,
        "exclude_patterns": exclude_patterns,
        "include_patterns": include_patterns,
        "manifest_subdirectory": _normalize_s3_prefix(os.environ.get("MANIFEST_SUBDIRECTORY", "_datasync/manifests/")),
        "override_options": override_options,
        "source_bucket_name": source_bucket_name,
        "source_prefix": _normalize_s3_prefix(os.environ.get("SOURCE_PREFIX", "")),
        "task_arn": task_arn,
        "transfer_last_modified_before": transfer_last_modified_before,
    }


def _is_datasync_task_execution_event(event_payload):
    return (
        event_payload.get("source") == DATASYNC_EVENT_SOURCE
        and event_payload.get("detail-type") == DATASYNC_TASK_EXECUTION_DETAIL_TYPE
    )


def _handle_datasync_success_event(event_payload):
    if not _parse_bool(os.environ.get("DELETE_TRANSFERRED_SOURCE_OBJECTS", "true")):
        logger.info("La limpieza del origen esta deshabilitada; se ignora el evento de exito")
        return {
            "statusCode": 200,
            "body": {
                "message": "La limpieza del origen esta deshabilitada",
            },
        }

    resources = event_payload.get("resources") or [None]
    execution_arn = resources[0]
    if not execution_arn:
        raise ValueError("El evento de DataSync no contiene TaskExecutionArn en resources[0]")

    datasync_client = boto3.client("datasync")
    s3_client = boto3.client("s3")

    task_execution_description = datasync_client.describe_task_execution(TaskExecutionArn=execution_arn)
    manifest_source = task_execution_description.get("ManifestConfig", {}).get("Source", {}).get("S3", {})
    manifest_bucket_arn = manifest_source.get("S3BucketArn")
    manifest_object_key = manifest_source.get("ManifestObjectPath")

    if not manifest_bucket_arn or not manifest_object_key:
        logger.info("La ejecucion %s no uso manifest; no se elimina nada del origen", execution_arn)
        return {
            "statusCode": 200,
            "body": {
                "message": "La ejecucion no uso manifest; no se limpiaron objetos del origen",
                "taskExecutionArn": execution_arn,
            },
        }

    manifest_bucket_name = _extract_bucket_name_from_arn(manifest_bucket_arn)
    manifest_entries = _read_manifest_entries(s3_client, manifest_bucket_name, manifest_object_key)

    source_bucket_name = os.environ.get("SOURCE_BUCKET_NAME", "").strip()
    source_prefix = _normalize_s3_prefix(os.environ.get("SOURCE_PREFIX", ""))
    deleted_object_count = _delete_source_objects(s3_client, source_bucket_name, source_prefix, manifest_entries)
    _restore_source_folder_markers(s3_client, source_bucket_name, source_prefix, manifest_entries)

    logger.info("Se eliminaron %s objetos del origen para la ejecucion %s", deleted_object_count, execution_arn)
    return {
        "statusCode": 200,
        "body": {
            "deletedObjectCount": deleted_object_count,
            "message": "Objetos transferidos eliminados del origen exitosamente",
            "taskExecutionArn": execution_arn,
        },
    }


def _start_datasync_transfer(event_payload):
    transfer_request = _build_transfer_request(event_payload)
    s3_client = boto3.client("s3")
    manifest_entries = _list_eligible_source_objects(
        s3_client=s3_client,
        source_bucket_name=transfer_request["source_bucket_name"],
        source_prefix=transfer_request["source_prefix"],
        transfer_last_modified_before=transfer_request["transfer_last_modified_before"],
        include_patterns=transfer_request["include_patterns"],
        exclude_patterns=transfer_request["exclude_patterns"],
    )

    if event_payload.get("dryRun"):
        logger.info("Ejecucion dryRun para el modo %s", transfer_request["execution_mode"])
        return {
            "statusCode": 200,
            "body": {
                "destinationBucketName": transfer_request["destination_bucket_name"],
                "destinationPrefix": transfer_request["destination_prefix"],
                "eligibleObjectCount": len(manifest_entries),
                "executionMode": transfer_request["execution_mode"],
                "message": "Dry run completado; no se ejecuto DataSync",
                "sampleObjects": manifest_entries[:20],
                "sourceBucketName": transfer_request["source_bucket_name"],
                "sourcePrefix": transfer_request["source_prefix"],
                "transferLastModifiedBefore": (
                    transfer_request["transfer_last_modified_before"].isoformat()
                    if transfer_request["transfer_last_modified_before"]
                    else None
                ),
            },
        }

    if not manifest_entries:
        logger.info("No hay objetos elegibles para transferir en el modo %s", transfer_request["execution_mode"])
        return {
            "statusCode": 200,
            "body": {
                "eligibleObjectCount": 0,
                "executionMode": transfer_request["execution_mode"],
                "message": "No hay objetos elegibles para transferir",
            },
        }

    manifest_object_key = _build_manifest_key(
        transfer_request["manifest_subdirectory"],
        transfer_request["execution_mode"],
    )
    _upload_manifest(
        s3_client=s3_client,
        destination_bucket_name=transfer_request["destination_bucket_name"],
        manifest_object_key=manifest_object_key,
        manifest_entries=manifest_entries,
    )

    start_request = {
        "TaskArn": transfer_request["task_arn"],
        "ManifestConfig": _build_manifest_config(
            bucket_name=transfer_request["destination_bucket_name"],
            manifest_object_key=manifest_object_key,
            bucket_access_role_arn=transfer_request["datasync_access_role_arn"],
        ),
    }

    if transfer_request["override_options"]:
        start_request["OverrideOptions"] = transfer_request["override_options"]

    datasync_client = boto3.client("datasync")

    try:
        logger.info(
            "Intentando iniciar la ejecucion de DataSync en modo %s para la tarea %s",
            transfer_request["execution_mode"],
            start_request["TaskArn"],
        )
        response = datasync_client.start_task_execution(**start_request)
        execution_arn = response["TaskExecutionArn"]
        logger.info("DataSync inicio correctamente. ID de ejecucion: %s", execution_arn)

        return {
            "statusCode": 200,
            "body": {
                "destinationBucketName": transfer_request["destination_bucket_name"],
                "destinationPrefix": transfer_request["destination_prefix"],
                "eligibleObjectCount": len(manifest_entries),
                "executionArn": execution_arn,
                "executionMode": transfer_request["execution_mode"],
                "manifestObjectKey": manifest_object_key,
                "message": "Ejecucion de DataSync iniciada exitosamente",
                "sourceBucketName": transfer_request["source_bucket_name"],
                "sourcePrefix": transfer_request["source_prefix"],
                "taskArn": start_request["TaskArn"],
            },
        }

    except datasync_client.exceptions.InvalidRequestException as error:
        logger.warning("No se pudo iniciar la tarea de DataSync: %s", error)
        return {
            "statusCode": 400,
            "body": {
                "executionMode": transfer_request["execution_mode"],
                "message": f"Solicitud invalida para DataSync: {str(error)}",
                "taskArn": start_request["TaskArn"],
            },
        }


def lambda_handler(event, context):
    """
    Orquesta ejecuciones de DataSync y, tras una ejecucion exitosa, limpia
    el origen para cumplir semantica de traslado.
    """

    try:
        normalized_event = _normalize_event(event)
        if _is_datasync_task_execution_event(normalized_event):
            if normalized_event.get("detail", {}).get("State") == DATASYNC_SUCCESS_STATE:
                return _handle_datasync_success_event(normalized_event)

            logger.info("Evento DataSync recibido pero no requiere accion: %s", normalized_event)
            return {
                "statusCode": 200,
                "body": {
                    "message": "Evento DataSync ignorado por no ser SUCCESS",
                },
            }

        return _start_datasync_transfer(normalized_event)

    except ValueError as error:
        logger.error("Solicitud invalida para la Lambda orquestadora: %s", error)
        return {
            "statusCode": 400,
            "body": {
                "message": str(error),
            },
        }

    except Exception as error:
        logger.error("Error fatal al ejecutar el orquestador DataSync: %s", error)
        raise
