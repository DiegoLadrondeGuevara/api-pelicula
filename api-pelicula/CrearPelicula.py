import boto3
import uuid
import os
import json
import traceback

def _log_info(data):
    return {"tipo": "INFO", "log_datos": data}

def _log_error(data):
    return {"tipo": "ERROR", "log_datos": data}

def lambda_handler(event, context):
    try:
        # Normalizar el body (API Gateway lo manda como string)
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event.get("body")

        if body is None:
            raise ValueError("No se recibió 'body' en el evento.")

        tenant_id = body["tenant_id"]
        pelicula_datos = body["pelicula_datos"]
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            "tenant_id": tenant_id,
            "uuid": uuidv4,
            "pelicula_datos": pelicula_datos
        }

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Log de éxito
        print(json.dumps(_log_info({
            "mensaje": "Película creada correctamente",
            "request_id": context.aws_request_id,
            "pelicula": pelicula
        })))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "mensaje": "Película creada",
                "pelicula": pelicula,
                "put_response": response
            })
        }

    except Exception as e:
        error_log = {
            "mensaje": str(e),
            "input_event": event,
            "traceback": traceback.format_exc()
        }

        # Log de error
        print(json.dumps(_log_error(error_log)))

        return {
            "statusCode": 400,
            "body": json.dumps({
                "mensaje": "Error al crear la película",
                "error": str(e)
            })
        }
