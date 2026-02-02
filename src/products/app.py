import json
import os
import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["PRODUCTS_TABLE"]

def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    # Simple pour l'entraînement : scan retourne tous les produits
    resp = table.scan()
    items = resp.get("Items", [])

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(items)
    }
