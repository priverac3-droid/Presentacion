import boto3
import os
import logging

# =============================================================================
# CONFIGURACIÓN DE LOGS
# =============================================================================
# Inicializamos el logger para que todo lo que imprimamos salga en CloudWatch.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Función Principal (Entry Point).
    Se encarga de recibir la señal y ordenar a AWS DataSync que inicie la tarea.
    """

    # -------------------------------------------------------------------------
    # 1. OBTENER CONFIGURACIÓN (DINÁMICA)
    # -------------------------------------------------------------------------
    # Leemos la variable de entorno que definimos en el CloudFormation (Sección 6).
    # Si estamos en DEV, traerá el ARN de Dev. Si es PROD, el de Prod.
    task_arn = os.environ.get('DATASYNC_TASK_ARN')
    
    # VALIDACIÓN DE SEGURIDAD:
    # Si por error la infraestructura no inyectó la variable, fallamos rápido.
    if not task_arn:
        logger.error("ERROR CRÍTICO: La variable de entorno 'DATASYNC_TASK_ARN' no existe.")
        return {
            'statusCode': 500,
            'body': 'Error de configuración interna: Falta DATASYNC_TASK_ARN'
        }

    # -------------------------------------------------------------------------
    # 2. INICIAR CLIENTE AWS
    # -------------------------------------------------------------------------
    # Creamos la conexión con el servicio de DataSync.
    client = boto3.client('datasync')

    try:
        logger.info(f"Intentando iniciar la ejecución de la tarea: {task_arn}")
        
        # ---------------------------------------------------------------------
        # 3. EJECUTAR LA TAREA
        # ---------------------------------------------------------------------
        # Esta es la llamada clave que inicia la replicación.
        response = client.start_task_execution(
            TaskArn=task_arn
        )
        
        # Obtenemos el ID de la ejecución para rastrearla
        execution_arn = response['TaskExecutionArn']
        logger.info(f"¡Éxito! Tarea iniciada correctamente. ID Ejecución: {execution_arn}")

        # Retornamos éxito (HTTP 200)
        return {
            'statusCode': 200,
            'body': {
                'message': 'Ejecución de DataSync iniciada exitosamente',
                'taskArn': task_arn,
                'executionArn': execution_arn
            }
        }

    except client.exceptions.InvalidRequestException as e:
        # ---------------------------------------------------------------------
        # MANEJO DE ERROR: TAREA YA EN USO (HTTP 400)
        # ---------------------------------------------------------------------
        # DataSync no permite correr la misma tarea dos veces al mismo tiempo.
        # Si intentamos lanzarla mientras corre, AWS devuelve InvalidRequestException.
        logger.warning(f"Advertencia: No se pudo iniciar. Probablemente ya esté corriendo. Detalle: {str(e)}")
        return {
            'statusCode': 400,
            'body': f"Solicitud inválida (¿Tarea ya en ejecución?): {str(e)}"
        }
        
    except Exception as e:
        # ---------------------------------------------------------------------
        # MANEJO DE ERROR: FALLO GENÉRICO (HTTP 500)
        # ---------------------------------------------------------------------
        # Cualquier otro error (permisos, red, caída de AWS).
        logger.error(f"Error fatal no controlado: {str(e)}")
        # Lanzamos la excepción para que Lambda marque la invocación como 'Failed'
        raise e