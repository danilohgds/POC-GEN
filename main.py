import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal


def lambda_handler(event, context):
    host = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')
    user = os.getenv('DB_USR')
    password = os.getenv('DB_PW')
    port = os.getenv('DB_PORT', 5432)

    http_method = event['httpMethod']
    path = event['path']
    query_params = event.get('queryStringParameters', {})

    # Extract ID from the path, if present
    id_from_path = None
    if path.startswith('/produto/'):
        id_from_path = path.split('/')[-1]

    nome_query_param = query_params.get('nome', None)
    categoria_query_param = query_params.get('categoria', None)

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Query by ID
        if id_from_path is not None:
            cursor.execute("SELECT * FROM Produto WHERE id = %s", (id_from_path,))

        # Query by 'nome' and 'categoria' if both are provided
        elif nome_query_param is not None and categoria_query_param is not None:
            cursor.execute("""
                SELECT * FROM Produto 
                WHERE nome ILIKE %s AND categoria_id = %s
            """, (f'%{nome_query_param}%', categoria_query_param))

        # Query by 'nome' if only 'nome' is provided
        elif nome_query_param is not None:
            cursor.execute("SELECT * FROM Produto WHERE nome ILIKE %s", (f'%{nome_query_param}%',))

        # Query by 'categoria' if only 'categoria' is provided
        elif categoria_query_param is not None:
            cursor.execute("SELECT * FROM Produto WHERE categoria_id = %s", (categoria_query_param,))

        else:
            return {
                'statusCode': 400,
                'body': 'Bad Request: Either id, nome, or categoria query parameters must be provided.'
            }

        # Fetch the result
        result = cursor.fetchall()

        if not result:
            return {
                'statusCode': 404,
                'body': 'Product not found.'
            }

        # Convert Decimal to float for JSON serialization
        def decimal_default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError("Type not serializable")

        # Return the result as JSON
        return {
            'statusCode': 200,
            'body': result  # Using custom serialization function
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error fetching data: {str(e)}'
        }

    finally:
        cursor.close()
        conn.close()

#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     event_aws = {
#         "body": "eyJ0ZXN0IjoiYm9keSJ9",
#         "resource": "/{proxy+}",
#         "path": "/produto/9",
#         "httpMethod": "POST",
#         "isBase64Encoded": 'true',
#         "queryStringParameters": {
#             "foo": "bar"
#         },
#         "multiValueQueryStringParameters": {
#             "foo": [
#                 "bar"
#             ]
#         },
#         "pathParameters": {
#             "proxy": "/path/to/resource"
#         },
#         "stageVariables": {
#             "baz": "qux"
#         },
#         "headers": {
#             "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#             "Accept-Encoding": "gzip, deflate, sdch",
#             "Accept-Language": "en-US,en;q=0.8",
#             "Cache-Control": "max-age=0",
#             "CloudFront-Forwarded-Proto": "https",
#             "CloudFront-Is-Desktop-Viewer": "true",
#             "CloudFront-Is-Mobile-Viewer": "false",
#             "CloudFront-Is-SmartTV-Viewer": "false",
#             "CloudFront-Is-Tablet-Viewer": "false",
#             "CloudFront-Viewer-Country": "US",
#             "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
#             "Upgrade-Insecure-Requests": "1",
#             "User-Agent": "Custom User Agent String",
#             "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
#             "X-Amz-Cf-Id": "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
#             "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
#             "X-Forwarded-Port": "443",
#             "X-Forwarded-Proto": "https"
#         },
#         "multiValueHeaders": {
#             "Accept": [
#                 "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
#             ],
#             "Accept-Encoding": [
#                 "gzip, deflate, sdch"
#             ],
#             "Accept-Language": [
#                 "en-US,en;q=0.8"
#             ],
#             "Cache-Control": [
#                 "max-age=0"
#             ],
#             "CloudFront-Forwarded-Proto": [
#                 "https"
#             ],
#             "CloudFront-Is-Desktop-Viewer": [
#                 "true"
#             ],
#             "CloudFront-Is-Mobile-Viewer": [
#                 "false"
#             ],
#             "CloudFront-Is-SmartTV-Viewer": [
#                 "false"
#             ],
#             "CloudFront-Is-Tablet-Viewer": [
#                 "false"
#             ],
#             "CloudFront-Viewer-Country": [
#                 "US"
#             ],
#             "Host": [
#                 "0123456789.execute-api.us-east-1.amazonaws.com"
#             ],
#             "Upgrade-Insecure-Requests": [
#                 "1"
#             ],
#             "User-Agent": [
#                 "Custom User Agent String"
#             ],
#             "Via": [
#                 "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)"
#             ],
#             "X-Amz-Cf-Id": [
#                 "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA=="
#             ],
#             "X-Forwarded-For": [
#                 "127.0.0.1, 127.0.0.2"
#             ],
#             "X-Forwarded-Port": [
#                 "443"
#             ],
#             "X-Forwarded-Proto": [
#                 "https"
#             ]
#         },
#         "requestContext": {
#             "accountId": "123456789012",
#             "resourceId": "123456",
#             "stage": "prod",
#             "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
#             "requestTime": "09/Apr/2015:12:34:56 +0000",
#             "requestTimeEpoch": 1428582896000,
#             "identity": {
#                 "cognitoIdentityPoolId": None,
#                 "accountId": None,
#                 "cognitoIdentityId": None,
#                 "caller": None,
#                 "accessKey": None,
#                 "sourceIp": "127.0.0.1",
#                 "cognitoAuthenticationType": None,
#                 "cognitoAuthenticationProvider": None,
#                 "userArn": None,
#                 "userAgent": "Custom User Agent String",
#                 "user": None
#             },
#             "path": "/prod/path/to/resource",
#             "resourcePath": "/{proxy+}",
#             "httpMethod": "POST",
#             "apiId": "1234567890",
#             "protocol": "HTTP/1.1"
#         }
#     }
#     lambda_handler(event_aws, None)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
