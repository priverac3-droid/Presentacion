# 📘 Manual de Despliegue: Migración ACH (DataSync + Orquestador)

**Proyecto:** Replicación Transversal S3-to-S3 con Orquestador Serverless
**Tecnología:** AWS CloudFormation + DataSync + **Lambda (Python 3.12)** + Azure DevOps
**Versión:** 2.0 (Con Orquestador Automático)

---

## 🏗️ Arquitectura

La solución consta de dos componentes principales:

1. **Canal de Datos (DataSync):** Mueve los archivos de la cuenta Origen a Destino.
2. **Orquestador (Lambda):** Una función Python que inicia la tarea de DataSync de forma programática y controlada, evitando ejecuciones simultáneas.

---

## ⚠️ 1. PRERREQUISITO DE SEGURIDAD (CRÍTICO)

Antes de ejecutar cualquier despliegue, la cuenta de **Desarrollo (Origen - `507781971948`)** debe autorizar a QA y Producción para leer sus datos.

**Acción:** Ir al Bucket `dev3-integrations...` -> Permissions -> **Bucket Policy** y pegar:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PermitirLecturaCrossAccountACH",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::462297762050:root",
                    "arn:aws:iam::817987897650:root"
                ]
            },
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:GetObject",
                "s3:GetObjectTagging"
            ],
            "Resource": [
                "arn:aws:s3:::dev3-integrations-t1-batch-outputfiledirectory-507781971948",
                "arn:aws:s3:::dev3-integrations-t1-batch-outputfiledirectory-507781971948/*"
            ]
        }
    ]
}

```

---

## 🤖 2. DESPLIEGUE AUTOMÁTICO (RECOMENDADO)

El despliegue se gestiona mediante **Azure DevOps**.

1. Hacer commit en la rama `trunk`.
2. El Pipeline `azure-pipelinePYTHON_v1_TRUNK.yml` se activará automáticamente.
3. **Flujo de ejecución:**
* Instalación de dependencias y parches de seguridad.
* Ejecución de Pruebas Unitarias (Coverage 100%).
* Empaquetado del código Lambda (`.zip`).
* Subida del artefacto a S3 (`b1-useast1-devops-artifacts-507781971948`).
* Despliegue de CloudFormation inyectando el código dinámicamente.



---

## 🛠️ 3. DESPLIEGUE MANUAL (DEBUGGING / EMERGENCY)

**⚠️ IMPORTANTE:** Si necesita desplegar manualmente desde su PC, **debe inyectar la ubicación del código Lambda**, de lo contrario el Stack fallará.

*Asumimos que existe un ZIP base en el bucket de artefactos. Si no, súbalo primero.*

### A) Despliegue en CALIDAD (QA03) - Cuenta `462297762050`

```bash
export AWS_PROFILE=462297762050_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
    --template-file infra/ach-datasync-master.yaml \
    --stack-name ACH-Migracion-Stack-QA \
    --parameter-overrides \
        file://infra/parameters/params-qa03.json \
        BucketCodigoLambda="b1-useast1-devops-artifacts-507781971948" \
        KeyCodigoLambda="ach-datasync/lambda/lambda-deploy.zip" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region us-east-1

```

### B) Despliegue en PRODUCCIÓN (PDN) - Cuenta `817987897650`

```bash
export AWS_PROFILE=817987897650_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
    --template-file infra/ach-datasync-master.yaml \
    --stack-name ACH-Migracion-Stack-PDN \
    --parameter-overrides \
        file://infra/parameters/params-pdn.json \
        BucketCodigoLambda="b1-useast1-devops-artifacts-507781971948" \
        KeyCodigoLambda="ach-datasync/lambda/lambda-deploy.zip" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region us-east-1

```

### C) Despliegue en DESARROLLO (DEV3) - Cuenta `507781971948`

```bash
export AWS_PROFILE=507781971948_BI-FSDEVELOPERROLE-QAPR

aws cloudformation deploy \
    --template-file infra/ach-datasync-master.yaml \
    --stack-name ACH-Migracion-Stack-DEV \
    --parameter-overrides \
        file://infra/parameters/params-dev3.json \
        BucketCodigoLambda="b1-useast1-devops-artifacts-507781971948" \
        KeyCodigoLambda="ach-datasync/lambda/lambda-deploy.zip" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region us-east-1

```

---

## ✅ 4. VALIDACIÓN Y OPERACIÓN

### 4.1 Verificar Infraestructura

1. Ir a CloudFormation y confirmar que el Stack está en **CREATE_COMPLETE** o **UPDATE_COMPLETE**.
2. Verificar que se creó la Lambda `ach-datasync-trigger-[ambiente]`.

### 4.2 Ejecución de la Migración (NUEVO MÉTODO)

Ya no es necesario iniciar la tarea manualmente en DataSync. Use el orquestador Lambda:

**Opción A: Desde Consola AWS**

1. Ir a **Lambda** -> Funciones -> `ach-datasync-trigger-[amb]`.
2. Pestaña **Test**.
3. Crear un evento vacío `{}` y dar clic en **Test**.
4. Debe responder `200 OK` y el `ExecutionArn` de DataSync.

**Opción B: Desde CLI**

```bash
aws lambda invoke \
    --function-name ach-datasync-trigger-dev3 \
    --payload '{}' \
    response.json

```

### 4.3 Monitoreo

* **Logs de Ejecución:** CloudWatch -> Log Groups -> `/aws/lambda/ach-datasync-trigger-[amb]`.
* **Estado de Transferencia:** DataSync -> History.
* **Notificaciones:** Revisar email `paul.rivera@banistmo.com` para alertas de éxito/fallo.