import pytest
import os
from unittest.mock import MagicMock, patch
# Importamos la función lambda_handler desde nuestra carpeta src
from src.index import lambda_handler

# =============================================================================
# PRUEBAS UNITARIAS (COVERAGE 100%)
# =============================================================================

@patch('boto3.client')
def test_lambda_success(mock_boto_client):
    """
    CASO 1: CAMINO FELIZ
    Simula que AWS responde correctamente e inicia la tarea.
    """
    # 1. Preparamos el "Actor" (Mock) que fingirá ser DataSync
    mock_datasync = MagicMock()
    mock_boto_client.return_value = mock_datasync
    
    # 2. Le decimos al actor qué debe responder cuando lo llamen
    mock_datasync.start_task_execution.return_value = {
        'TaskExecutionArn': 'arn:aws:datasync:us-east-1:123456789012:execution/exec-001'
    }
    
    # 3. Configuramos el entorno (Simulamos la variable que inyecta CloudFormation)
    os.environ['DATASYNC_TASK_ARN'] = 'arn:aws:datasync:us-east-1:123456789012:task/task-001'
    
    # 4. EJECUTAMOS LA LAMBDA
    response = lambda_handler({}, {})
    
    # 5. VERIFICACIONES (ASSERTS)
    assert response['statusCode'] == 200
    assert 'exec-001' in str(response['body'])
    # Verificamos que sí se llamó a la función de AWS exactamente una vez
    mock_datasync.start_task_execution.assert_called_once()

def test_lambda_missing_env_var():
    """
    CASO 2: ERROR DE CONFIGURACIÓN
    Simula que olvidamos poner la variable de entorno en el CloudFormation.
    """
    # 1. Borramos la variable a propósito
    if 'DATASYNC_TASK_ARN' in os.environ:
        del os.environ['DATASYNC_TASK_ARN']
        
    # 2. Ejecutamos
    response = lambda_handler({}, {})
    
    # 3. Verificamos que falle controladamente (Error 500 pero manejado)
    assert response['statusCode'] == 500
    assert 'Error de configuración' in response['body']

@patch('boto3.client')
def test_lambda_invalid_request(mock_boto_client):
    """
    CASO 3: TAREA YA EN EJECUCIÓN (Error 400)
    Simula que AWS nos dice "Espera, ya estoy ocupado".
    """
    # Preparación del Mock
    mock_datasync = MagicMock()
    mock_boto_client.return_value = mock_datasync
    os.environ['DATASYNC_TASK_ARN'] = 'arn:aws:datasync:task-001'

    # --- TRUCO TÉCNICO PARA MOCKEAR EXCEPCIONES DE AWS ---
    # Como no estamos conectados a AWS, la clase 'client.exceptions' no existe.
    # Tenemos que crear una clase falsa que se llame igual para engañar al código.
    class MockInvalidRequestException(Exception):
        pass
    
    # Inyectamos esta clase falsa dentro de nuestro actor
    mock_datasync.exceptions.InvalidRequestException = MockInvalidRequestException
    
    # Le decimos al actor: "Cuando intenten ejecutar, LANZA este error"
    mock_datasync.start_task_execution.side_effect = MockInvalidRequestException("La tarea ya está corriendo")
    
    # Ejecutamos
    response = lambda_handler({}, {})
    
    # Verificamos que el código atrapó el error y devolvió 400 (no explotó)
    assert response['statusCode'] == 400
    assert 'La tarea ya está corriendo' in response['body']

@patch('boto3.client')
def test_lambda_generic_exception(mock_boto_client):
    """
    CASO 4: ERROR FATAL (Error 500)
    Simula una caída de red o error desconocido de AWS.
    """
    mock_datasync = MagicMock()
    mock_boto_client.return_value = mock_datasync
    os.environ['DATASYNC_TASK_ARN'] = 'arn:aws:datasync:task-001'

    # También definimos la excepción aquí para que no falle al intentar leer 'exceptions'
    class MockInvalidRequestException(Exception):
        pass
    mock_datasync.exceptions.InvalidRequestException = MockInvalidRequestException

    # Simulamos un error genérico cualquiera
    mock_datasync.start_task_execution.side_effect = Exception("Error de conexión fatal con AWS")
    
    # Verificamos que la Lambda deje pasar el error (raise) para que CloudWatch lo marque como FALLO
    with pytest.raises(Exception) as excinfo:
        lambda_handler({}, {})
    
    assert "Error de conexión fatal" in str(excinfo.value)