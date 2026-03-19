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

## 10. Flujo oficial de despliegue

El flujo operativo oficial de este desarrollo es:

1. correr el pipeline `azure-pipelinePYTHON_v1_TRUNK.yml`
2. validar que el build publique correctamente el artefacto
3. desplegar usando el release clasico de Azure DevOps

Este repositorio mantiene ese modelo. El YAML del pipeline queda dedicado a:

- instalar dependencias
- ejecutar pruebas
- correr SonarQube
- generar coverage
- empaquetar el repo como artefacto

El despliegue a AWS no se hace desde el YAML de build, sino desde el release.

### Release clasico esperado

Las etapas actualmente asociadas al proceso de despliegue son las del release
corporativo, por ejemplo:

- `INT-DEPLOY`
- `QA-DEPLOY`
- `PROD-DEPLOY`
- validaciones manuales / rollback

### Que ya quedo probado

- el pipeline compila y genera el artefacto
- el release ya paso correctamente en dev

Por eso los cambios del repositorio se mantuvieron compatibles con el modelo
`pipeline -> release`, sin mover el despliegue operativo al YAML de CI.

### Que no hace falta cambiar ahora en el release

Si dev ya desplego bien con el release actual, no hace falta tocar de inmediato:

- la secuencia general del release
- las aprobaciones manuales
- los stages actuales de dev / qa / prod

### Compatibilidad con stacks ya existentes

Si un ambiente ya fue desplegado antes con la version inicial del template y el
stack ya administra el bucket destino y la CMK asociados, mantén
`CrearBucketDestino=true` en ese ambiente. Cambiarlo a `false` durante un update
haría que CloudFormation intente retirar esos recursos del stack, lo que puede
provocar rollback si el bucket ya tiene informacion o si la llave KMS sigue en
uso.

### Que si habria que cambiar mas adelante si quieren multi-region desde release

Eso no es obligatorio para continuar con el flujo actual, pero si luego quieren
desplegar tambien a `us-west-2` desde el release, habria que agregar en el
release clasico:

- variables por region para bucket de artefactos
- una subida del ZIP por cada region
- una ejecucion adicional de `Create/Update Stack` por region

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

## 12. Pipeline de build

El pipeline `azure-pipelinePYTHON_v1_TRUNK.yml`:

1. instala dependencias
2. ejecuta pruebas
3. empaqueta la Lambda
4. publica el artefacto que luego consume el release

No despliega a AWS directamente. El despliegue lo hace el release clasico.

## 13. Notas operativas

- Para dev3 y pdn los parametros vienen preparados para reutilizar un bucket destino
- Para qa03 los parametros vienen preparados para crear el bucket destino si no existe
- Si el nombre real del bucket destino no coincide con la convencion estandar, define
  `BucketDestinoNombre`
- Si el bucket destino existente usa SSE-KMS, define `BucketDestinoKmsKeyArn`
- Si activas Object Lock, hazlo solo en buckets nuevos y con aprobacion previa
