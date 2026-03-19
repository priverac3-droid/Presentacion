# Respaldo Transversal ACH con DataSync + Lambda

Este repositorio implementa una solucion de respaldo S3-to-S3 usando solo:

- AWS DataSync
- AWS Lambda
- AWS EventBridge
- AWS SNS
- AWS CloudFormation

No usa AWS Glue en ninguna parte.

La solucion fue ajustada para cubrir estos requerimientos:

1. Trabajar por ambiente:
   - dev3 -> origen dev3 -> destino dev3
   - qa03 -> origen qa03 -> destino qa03
   - pdn -> origen pdn -> destino pdn
2. Soportar despliegue en Virginia (`us-east-1`) y Oregon (`us-west-2`)
3. Ejecutar una carga historica inicial y luego un batch diario
4. Reutilizar buckets destino existentes o crearlos si hace falta
5. Mantener buenas practicas de seguridad sin credenciales en texto plano

## 1. Estructura del proyecto

```text
.
├── src/
│   └── index.py
├── test/
│   └── test_index.py
├── infra/
│   ├── ach-datasync-master.yaml
│   └── parameters/
│       ├── params-dev3.json
│       ├── params-qa03.json
│       ├── params-pdn.json
│       └── prerequisito.txt
├── azure-pipelinePYTHON_v1_TRUNK.yml
├── requirements.txt
├── sonar-project.properties
└── permisos
```

## 2. Logica principal

La solucion queda dividida en dos flujos:

### Fase 1: carga historica

- La Lambda `ach-datasync-trigger-{ambiente}` se invoca manualmente con
  `executionMode=historical`
- La Lambda dispara la tarea `HistoricalDataSyncTask`
- Esa tarea ejecuta una copia completa (`TransferMode=ALL`)
- Se preserva el historico en destino y no se borra el origen

### Fase 2: batch diario

- EventBridge invoca la misma Lambda con `executionMode=daily`
- La Lambda dispara la tarea `DailyDataSyncTask`
- Esa tarea usa `TransferMode=CHANGED`
- Solo replica cambios nuevos o modificados

## 3. Arquitectura resultante

Por cada ambiente y por cada region desplegada, el stack crea o reutiliza:

- un bucket destino transversal
- una CMK KMS si el bucket destino lo crea el stack
- una ubicacion origen DataSync
- una ubicacion destino DataSync
- una tarea DataSync historica
- una tarea DataSync diaria
- una Lambda orquestadora
- una regla EventBridge para el batch diario
- reportes DataSync en S3
- alertas SNS por fallo

## 4. Buckets origen por ambiente

Los parametros versionados en el repositorio quedan asi:

| Ambiente | Bucket origen |
|---|---|
| dev3 | `dev3-integrations-t1-batch-outputfiledirectory-507781971948` |
| qa03 | `qa03-integrations-t1-batch-outputfiledirectory-462297762050` |
| pdn | `prod-integrations-t1-batch-outputfiledirectory-817987897650` |

La ruta origen usada por defecto es:

```text
/replication/ACH/
```

## 5. Destino por ambiente y region

Si no se indica `BucketDestinoNombre`, la plantilla usa esta convencion:

```text
b1-[region-short]-[ambiente]-coreb-backuptransversal-[cuenta]
```

Ejemplos:

- `b1-useast1-dev3-coreb-backuptransversal-507781971948`
- `b1-useast1-qa03-coreb-backuptransversal-462297762050`
- `b1-uswest2-pdn-coreb-backuptransversal-817987897650`

La ruta destino usada por defecto es:

```text
/ACH/
```

DataSync crea el prefijo si no existe. No hace falta crear carpetas manualmente.

## 6. Seguridad

### Principios aplicados

- no se guardan credenciales AWS ni contrasenas en el repositorio
- la solucion usa roles IAM para Lambda y DataSync
- el pipeline usa service connections externas
- la Lambda puede leer configuracion adicional desde Secrets Manager usando
  `OrchestratorSecretArn`
- si reutilizas un bucket KMS existente, debes pasar `BucketDestinoKmsKeyArn`
- el bucket creado por el stack queda:
  - cifrado con KMS
  - con versionado habilitado
  - con bloqueo de acceso publico
  - con lifecycle a Deep Archive

### Secrets Manager

Si necesitas overrides operativos sin usar variables planas, la Lambda acepta un
secreto JSON opcional. Ejemplo de estructura:

```json
{
  "defaultIncludePatterns": "/Listado|/reportes",
  "historical": {
    "includePatterns": "/Listado|/reportes|/TDIR_IN_ERR|/TDIR_IN_HIS|/TDIR_OUT_HIS|/TDIR-OUT|/TRTP-IN|/TRTP-OUT",
    "overrideOptions": {
      "BytesPerSecond": 52428800
    }
  },
  "daily": {
    "overrideOptions": {
      "BytesPerSecond": 20971520
    }
  }
}
```

Ese secreto es opcional. La solucion funciona tambien solo con variables de
entorno inyectadas por CloudFormation.

Cuando uses `OrchestratorSecretArn`, pasa el ARN completo del secreto. La Lambda
usa la region embebida en el ARN para leer el secreto correctamente incluso en
despliegues multi-region. Los valores del secreto tienen prioridad sobre las
variables de entorno del modo correspondiente.

## 7. Auditoria y observabilidad

La solucion deja trazabilidad en varias capas:

- CloudWatch Logs para Lambda
- CloudWatch Logs para DataSync
- task reports de DataSync en:

```text
/reports/ach/
```

- SNS con notificacion por email cuando una tarea falle

Los task reports permiten verificar que la copia fue ejecutada y que los objetos
transferidos o validados quedaron registrados.

## 8. Lifecycle y retencion

Para buckets creados por el stack:

- 0 a 90 dias: almacenamiento normal
- desde dia 91: `DEEP_ARCHIVE`
- expiracion automatica: 2920 dias (~8 anos)

Object Lock queda disponible como opcion (`HabilitarObjectLock=true`) solo al
crear buckets nuevos. No se fuerza por defecto porque es irreversible.

## 9. Parametros importantes

| Parametro | Uso |
|---|---|
| `BucketOrigenNombre` | Bucket origen del ambiente |
| `RutaOrigenACH` | Prefijo origen |
| `RutaDestinoACH` | Prefijo destino |
| `CrearBucketDestino` | Crear o reutilizar bucket destino |
| `BucketDestinoNombre` | Nombre explicito del bucket destino |
| `BucketDestinoKmsKeyArn` | CMK del bucket destino existente |
| `ScheduleExpressionDaily` | Cron del batch diario |
| `HistoricalThrottleBytesPerSecond` | Limite historico |
| `DailyThrottleBytesPerSecond` | Limite diario |
| `TaskReportSubdirectory` | Ruta de reportes |
| `OrchestratorSecretArn` | Secreto opcional del orquestador |

## 10. Despliegue manual

Antes de ejecutar un despliegue manual:

1. sube el ZIP de Lambda a un bucket de artefactos en la misma region del stack
2. usa la key real publicada, por ejemplo `ach-datasync/lambda/lambda-code-<BuildId>.zip`

### dev3

#### Virginia

```bash
export AWS_PROFILE=507781971948_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
  --template-file infra/ach-datasync-master.yaml \
  --stack-name ACH-Replicacion-dev3 \
  --parameter-overrides file://infra/parameters/params-dev3.json \
  BucketCodigoLambda="b1-useast1-devops-artifacts-507781971948" \
  KeyCodigoLambda="ach-datasync/lambda/lambda-code-<BuildId>.zip" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

#### Oregon

```bash
export AWS_PROFILE=507781971948_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
  --template-file infra/ach-datasync-master.yaml \
  --stack-name ACH-Replicacion-dev3 \
  --parameter-overrides file://infra/parameters/params-dev3.json \
  BucketCodigoLambda="b1-uswest2-devops-artifacts-507781971948" \
  KeyCodigoLambda="ach-datasync/lambda/lambda-code-<BuildId>.zip" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-west-2
```

### qa03

```bash
export AWS_PROFILE=462297762050_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
  --template-file infra/ach-datasync-master.yaml \
  --stack-name ACH-Replicacion-qa03 \
  --parameter-overrides file://infra/parameters/params-qa03.json \
  BucketCodigoLambda="b1-useast1-devops-artifacts-507781971948" \
  KeyCodigoLambda="ach-datasync/lambda/lambda-code-<BuildId>.zip" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

### pdn

```bash
export AWS_PROFILE=817987897650_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
  --template-file infra/ach-datasync-master.yaml \
  --stack-name ACH-Replicacion-pdn \
  --parameter-overrides file://infra/parameters/params-pdn.json \
  BucketCodigoLambda="b1-useast1-devops-artifacts-507781971948" \
  KeyCodigoLambda="ach-datasync/lambda/lambda-code-<BuildId>.zip" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

## 11. Ejecucion operativa

### Carga historica manual

```bash
aws lambda invoke \
  --function-name ach-datasync-trigger-dev3 \
  --payload '{"executionMode":"historical"}' \
  response.json
```

### Carga historica filtrando subcarpetas conocidas

```bash
aws lambda invoke \
  --function-name ach-datasync-trigger-dev3 \
  --payload '{
    "executionMode": "historical",
    "includePatterns": [
      "Listado",
      "reportes",
      "TDIR_IN_ERR",
      "TDIR_IN_HIS",
      "TDIR_OUT_HIS",
      "TDIR-OUT",
      "TRTP-IN",
      "TRTP-OUT"
    ]
  }' \
  response.json
```

### Dry run de validacion

```bash
aws lambda invoke \
  --function-name ach-datasync-trigger-dev3 \
  --payload '{"executionMode":"historical","dryRun":true}' \
  response.json
```

### Batch diario

No requiere invocacion manual cuando `EnableDailySchedule=true`, porque
EventBridge ejecuta la Lambda con el cron configurado en `ScheduleExpressionDaily`.

## 12. Pipeline

El pipeline `azure-pipelinePYTHON_v1_TRUNK.yml`:

1. instala dependencias
2. ejecuta pruebas
3. empaqueta la Lambda
4. publica el ZIP en un bucket de artefactos por region
5. despliega CloudFormation en las regiones configuradas

## 13. Notas operativas

- Para dev3 y pdn los parametros vienen preparados para reutilizar un bucket destino
- Para qa03 los parametros vienen preparados para crear el bucket destino si no existe
- Si el nombre real del bucket destino no coincide con la convencion estandar, define
  `BucketDestinoNombre`
- Si el bucket destino existente usa SSE-KMS, define `BucketDestinoKmsKeyArn`
- Si activas Object Lock, hazlo solo en buckets nuevos y con aprobacion previa
