import json
import logging
import os
from functools import lru_cache

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

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


@lru_cache(maxsize=1)
def _load_secret_configuration():
    secret_arn = os.environ.get("ORCHESTRATOR_SECRET_ARN", "").strip()
    if not secret_arn:
        return {}

    logger.info("Cargando configuracion adicional desde Secrets Manager")
    response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
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


def _build_start_request(event_payload):
    execution_mode = str(event_payload.get("executionMode", "daily")).strip().lower()
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

    include_patterns = _normalize_filter_patterns(
        _first_non_empty(
            event_payload.get("includePatterns"),
            os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("include", ""), ""),
            mode_secret.get("includePatterns"),
            os.environ.get("DEFAULT_INCLUDE_PATTERNS", ""),
            secret_config.get("defaultIncludePatterns"),
        )
    )
    exclude_patterns = _normalize_filter_patterns(
        _first_non_empty(
            event_payload.get("excludePatterns"),
            os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("exclude", ""), ""),
            mode_secret.get("excludePatterns"),
            os.environ.get("DEFAULT_EXCLUDE_PATTERNS", ""),
            secret_config.get("defaultExcludePatterns"),
        )
    )

    env_override_options = _parse_json_object(
        os.environ.get(MODE_ENV_MAP.get(execution_mode, {}).get("override", ""), ""),
        MODE_ENV_MAP.get(execution_mode, {}).get("override", "override"),
    )
    override_options = DEFAULT_OVERRIDE_OPTIONS.get(execution_mode, {}).copy()
    override_options.update(_normalize_override_options(secret_config.get("defaultOverrideOptions", {})))
    override_options.update(_normalize_override_options(mode_secret.get("overrideOptions", {})))
    override_options.update(_normalize_override_options(env_override_options))
    override_options.update(_normalize_override_options(event_payload.get("overrideOptions", {})))

    start_request = {"TaskArn": task_arn}

    if include_patterns:
        start_request["Includes"] = [{
            "FilterType": "SIMPLE_PATTERN",
            "Value": include_patterns,
        }]

    if exclude_patterns:
        start_request["Excludes"] = [{
            "FilterType": "SIMPLE_PATTERN",
            "Value": exclude_patterns,
        }]

    if override_options:
        start_request["OverrideOptions"] = override_options

    return execution_mode, start_request


def lambda_handler(event, context):
    """
    Orquesta ejecuciones de DataSync para dos escenarios:
    - historical: carga inicial / backfill
    - daily: sincronizacion incremental diaria
    """

    try:
        normalized_event = _normalize_event(event)
        execution_mode, start_request = _build_start_request(normalized_event)
    except ValueError as error:
        logger.error("Solicitud invalida para la Lambda orquestadora: %s", error)
        return {
            "statusCode": 400,
            "body": {
                "message": str(error),
            },
        }

    if normalized_event.get("dryRun"):
        logger.info("Ejecucion dryRun para el modo %s", execution_mode)
        return {
            "statusCode": 200,
            "body": {
                "message": "Dry run completado; no se ejecuto DataSync",
                "executionMode": execution_mode,
                "startTaskExecutionRequest": start_request,
            },
        }

    client = boto3.client("datasync")

    try:
        logger.info(
            "Intentando iniciar la ejecucion de DataSync en modo %s para la tarea %s",
            execution_mode,
            start_request["TaskArn"],
        )
        response = client.start_task_execution(**start_request)
        execution_arn = response["TaskExecutionArn"]
        logger.info("DataSync inicio correctamente. ID de ejecucion: %s", execution_arn)

        return {
            "statusCode": 200,
            "body": {
                "executionArn": execution_arn,
                "executionMode": execution_mode,
                "message": "Ejecucion de DataSync iniciada exitosamente",
                "taskArn": start_request["TaskArn"],
            },
        }

    except client.exceptions.InvalidRequestException as error:
        logger.warning("No se pudo iniciar la tarea de DataSync: %s", error)
        return {
            "statusCode": 400,
            "body": {
                "executionMode": execution_mode,
                "message": f"Solicitud invalida para DataSync: {str(error)}",
                "taskArn": start_request["TaskArn"],
            },
        }

    except Exception as error:
        logger.error("Error fatal al ejecutar DataSync: %s", error)
        raise