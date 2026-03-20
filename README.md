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
- La Lambda lista los objetos del origen bajo `replication/ACH/`
- Filtra solo objetos con `LastModified` anterior a `2026-02-01T00:00:00Z`
- Genera un manifest CSV en `_datasync/manifests/`
- Dispara `HistoricalDataSyncTask` usando ese manifest
- Cuando DataSync termina en `SUCCESS`, la misma Lambda elimina del origen los
  objetos transferidos y deja marcadores de carpeta vacios

### Fase 2: batch diario

- EventBridge invoca la misma Lambda con `executionMode=daily`
- La Lambda vuelve a construir un manifest con los objetos elegibles
- DataSync transfiere solo el conjunto listado en el manifest
- Tras un `SUCCESS`, la Lambda limpia el origen para mantener semantica de traslado

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
- una regla EventBridge adicional para limpiar el origen tras un exito de DataSync
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
replication/ACH/
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
ACH/
```

Con `RutaDestinoACH=ACH/`, DataSync copia el contenido de
`replication/ACH/` hacia:

```text
bucket-destino/ACH/
```

**No se crea** una carpeta `replication/` en el destino: el prefijo `replication/ACH/`
del origen es solo la ruta de lectura; lo que va al transversal queda bajo `ACH/`.

### ¿Por qué también aparece `_datasync/` en el bucket destino?

Eso **no** es la copia del árbol `replication/` ni un fallo de mapeo. Son prefijos **operativos**, separados de la data ACH:

| Prefijo | Quién lo escribe | Propósito |
| --- | --- | --- |
| `ACH/` | DataSync | Datos de negocio (Listado, reportes, TDIR_*, TRTP-*, etc.) |
| `_datasync/manifests/` | Lambda | Manifiestos CSV que usa DataSync para saber qué objetos transferir |
| `_datasync/task-reports/` (p. ej. `Summary-Reports/`) | DataSync | Informes de ejecución / auditoría de la tarea |

Si en el origen debe quedar solo la **estructura de carpetas vacías** tras el traslado, eso lo hace la Lambda al borrar objetos transferidos y recrear marcadores de carpeta; el corte por **fecha** se controla con `TransferLastModifiedBefore` (objetos con `LastModified` en febrero 2026 o posteriores no entran al manifiesto histórico con el valor por defecto del template).

En el destino, bajo `ACH/`, la estructura típica queda así:

```text
ACH/Listado/
ACH/reportes/
ACH/TDIR_IN_ERR/
ACH/TDIR_IN_HIS/
ACH/TDIR_OUT_HIS/
ACH/TDIR-OUT/
ACH/TRTP-IN/
ACH/TRTP-OUT/
```

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
- manifests de DataSync en:

```text
_datasync/manifests/
```
- task reports de DataSync en:

```text
_datasync/task-reports/
```

  (Si en el stack pones `TaskReportSubdirectory` vacío, DataSync no escribe esos informes en S3.)

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
| `RutaDestinoACH` | Prefijo destino; usar `ACH/` |
| `CrearBucketDestino` | Crear o reutilizar bucket destino |
| `BucketDestinoNombre` | Nombre explicito del bucket destino |
| `BucketDestinoKmsKeyArn` | CMK del bucket destino existente |
| `DeleteTransferredSourceObjects` | Elimina del origen lo ya transferido |
| `ManifestSubdirectory` | Ruta donde la Lambda escribe manifests |
| `ScheduleExpressionDaily` | Cron del batch diario |
| `HistoricalThrottleBytesPerSecond` | Limite historico |
| `DailyThrottleBytesPerSecond` | Limite diario |
| `TaskReportSubdirectory` | Ruta de reportes |
| `TransferLastModifiedBefore` | Corte por fecha para incluir solo historico |
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

Si un ambiente ya tiene bucket destino creado y quieres reutilizarlo, usa:

- `CrearBucketDestino=false`
- `BucketDestinoNombre=<bucket-existente>`

En dev3, los parametros quedaron preparados para reutilizar el bucket
`b1-useast1-dev3-coreb-backuptransversal-507781971948`.

### Ruta alternativa si el stack principal queda bloqueado

Si no puedes recuperar inmediatamente el stack principal y necesitas un despliegue
nuevo en dev, el template ahora soporta una instancia paralela mediante el
parámetro `ResourceNameSuffix`.

Para ese caso:

- usa un `stackName` nuevo en el release
- usa el archivo `infra/parameters/params-dev3-parallel.json`
- define una variable del release llamada `resourceNameSuffix`
- asigna un valor corto y unico, por ejemplo `-1` o `-int2`

Ese sufijo se agrega a los nombres fisicos de Lambda, roles IAM, alias KMS,
reglas EventBridge, tareas DataSync, topic SNS y bucket destino por defecto para
evitar colisiones con el stack principal.

### Parametros del codigo Lambda en el release

Los archivos `params-*.json` ya vienen preparados para que la task `Replace Tokens`
del release resuelva estos valores:

- `BucketCodigoLambda` -> `#{bucketName}#`
- `KeyCodigoLambda` -> `respaldo-transversal/#{Build.BuildId}#.zip`

Eso asume que la task usa el patron por defecto `#{variable}#` de Replace Tokens.
Si el release usa otro patron, hay que configurarlo para que coincida o ajustar
los tokens del archivo de parametros.

### Bucket S3 del ZIP (release) y error "Bucket does not exist"

La tarea **Amazon S3 Upload** del release debe apuntar a un bucket **existente** en
la cuenta y region correctas, con permisos para `PutObject`. El token `bucketName`
sustituye `BucketCodigoLambda` en los `params-*.json`; **no** es el bucket de datos
ACH ni el transversal.

**Cuentas destino por ambiente** (deben coincidir con `infra/parameters/`):

| Ambiente | ID de cuenta (ejemplo) |
| --- | --- |
| dev3 | `507781971948` |
| qa03 | `462297762050` |
| pdn | `817987897650` |

Si al correr el stage de **QA** el log muestra un bucket con sufijo **`-dev-`** y
cuenta `507781971948` (por ejemplo `aws-useast1-dev-507781971948`), el release
sigue usando la variable de **dev**: hay que definir **otra** variable de bucket
por stage (p. ej. `bucketNameQa`) o scope por ambiente, y que QA resuelva al
bucket de artefactos en la cuenta **462297762050**, no al de dev.

Si el nombre de bucket es el correcto pero el error continua: crear el bucket en
S3 en `us-east-1`, o habilitar la opcion de auto-creacion de la tarea solo si la
politica de la empresa lo permite. Verificar que el service connection asuma un
rol con acceso a ese bucket.

#### Ejemplo: log con `aws-useast1-qa-462297762050` y mismo error

Ese nombre ya apunta a **QA** (cuenta `462297762050`). El mensaje indica que el
bucket **no existe todavía** en AWS o que el rol del service connection **no**
tiene permiso sobre él (la tarea también hace `HeadBucket`).

**Opción A — Crear el bucket una vez** en la cuenta y region del release
(`us-east-1`), por consola S3 o CLI:

```bash
aws s3 mb s3://aws-useast1-qa-462297762050 --region us-east-1
```

**Opción B — Auto-create**: en la tarea **Amazon S3 Upload**, activar la opción
para **crear el bucket si no existe** (solo si gobierno de seguridad lo permite).

**Opción C — Permisos**: en IAM, al rol que asume el service connection, añadir
política mínima sobre ese bucket, por ejemplo:

- `s3:ListBucket` en `arn:aws:s3:::aws-useast1-qa-462297762050`
- `s3:PutObject`, `s3:AbortMultipartUpload`, `s3:DeleteObject` (si aplica) en
  `arn:aws:s3:::aws-useast1-qa-462297762050/*`

Sin bucket creado o sin `HeadBucket`/`PutObject`, la tarea fallará igual aunque
el nombre sea correcto.

### Destino en QA: no existe el bucket o no aparece la carpeta `ACH/`

Son cosas distintas:

1. **Bucket transversal (datos)** — No es el bucket del ZIP de la Lambda. Con
   `infra/parameters/params-qa03.json`, si `CrearBucketDestino=true` y
   `BucketDestinoNombre` esta vacio, el template crea (nombre por convencion en
   `us-east-1`):

   ```text
   b1-useast1-qa03-coreb-backuptransversal-462297762050
   ```

   Si ese bucket **no existe** en la cuenta `462297762050`, el stack de
   CloudFormation **no se ha aplicado** en QA, fallo al crear el bucket, o se
   desplego con `CrearBucketDestino=false` sin indicar un
   `BucketDestinoNombre` real. Hay que revisar el estado del stack y los
   parametros del release en QA.

2. **“Carpeta” `ACH/`** — En S3 no hay directorios; `ACH/` es un **prefijo**. La
   consola solo muestra esa ruta despues del **primer objeto** bajo
   `ACH/...` (por ejemplo tras una ejecucion exitosa de DataSync) o si alguien
   sube un marcador vacio:

   ```bash
   aws s3api put-object \
     --bucket b1-useast1-qa03-coreb-backuptransversal-462297762050 \
     --key ACH/.keep \
     --body ""
   ```

   Hasta entonces puede parecer que “no existe la carpeta de destino” aunque el
   bucket y la ubicacion DataSync esten bien.

### Estructura esperada del traspaso

La expectativa funcional correcta es:

- origen: `s3://.../replication/ACH/`
- destino: `s3://bucket-respaldo/ACH/`
- resultado: dentro del destino deben quedar carpetas funcionales bajo `ACH/`,
  como `ACH/Listado/`, `ACH/reportes/`, `ACH/TDIR_IN_ERR/`, `ACH/TRTP-IN/`, etc.
- del origen se trasladan los objetos elegibles y luego se eliminan, dejando las
  carpetas visibles pero sin data util
- no se deben mover objetos con fecha de febrero 2026 en adelante (según el corte configurado)

La data de respaldo vive solo bajo `ACH/`. Los manifests y task reports no se mezclan
con esas rutas: usan el prefijo `_datasync/`, que es solo trazabilidad técnica.

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
La Lambda vuelve a evaluar el corte por fecha y solo incluye en el manifest los
objetos elegibles antes de iniciar DataSync.

## 12. Pipeline de build

El pipeline `azure-pipelinePYTHON_v1_TRUNK.yml`:

1. instala dependencias
2. ejecuta pruebas
3. empaqueta la Lambda
4. publica el artefacto que luego consume el release

No despliega a AWS directamente. El despliegue lo hace el release clasico.

Si agregas una tarea **Amazon S3 Upload** que use `$(bucketName)`, define la
variable en el bloque `variables:` del YAML (como en este repo, p. ej.
`bucketName: aws-useast1-qa-462297762050`) o en la pestaña **Variables** del
**pipeline de build** en Azure DevOps. Sin eso, el build falla con “undefined
variable bucketName”.

**Build vs Release (clasico):** Las variables que ves en un **Release** con
**Scope** por etapa (p. ej. `INT-DEPLOY`, `QA-DEPLOY`) **no** se inyectan en el
**Build** YAML. Son contextos distintos: el job de compilacion no lee las
variables del release. Si la tarea S3 Upload esta en el **build**, hace falta
`bucketName` en el YAML o en Variables del **mismo** pipeline de build. Si la
tarea esta solo en el **release**, usa las variables con scope del stage
correspondiente y ahi no aplica el error del YAML de build.

**Valores por stage:** Revisa que el bucket en `QA-DEPLOY` exista y sea el
correcto para subir el ZIP (nombres corporativos tipo `*-cfn-sam-template-upload-*`
suelen ser distintos de `aws-useast1-qa-*`; deben coincidir con el bucket real
en S3).

## 13. Notas operativas

- Para dev3 los parametros vienen preparados para reutilizar el bucket destino existente
- Para qa03 y pdn puedes decidir entre reutilizar o crear bucket segun el ambiente
- Si el nombre real del bucket destino no coincide con la convencion estandar, define
  `BucketDestinoNombre`
- Si el bucket destino existente usa SSE-KMS, define `BucketDestinoKmsKeyArn`
- Si activas Object Lock, hazlo solo en buckets nuevos y con aprobacion previa
