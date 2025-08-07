import functools

from django.conf import settings
from django.http import HttpRequest, JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view

from google.protobuf.message import Message as ProtoMessage

from core.utils.error_utils import error_dict
from core.utils.proto_utils import json_to_proto, proto_to_dict
from core.utils.logging_utils import log_function_call


def proto_schema_validator(request_schema):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(request: HttpRequest):
            body = request.body
            try:
                if body:
                    request_message = json_to_proto(body, request_schema)
                else:
                    request_message = request_schema()
            except Exception as e:
                return JsonResponse(error_dict('Error while deserializing the proto msg', e), status=400,
                                    content_type='application/json')

            try:
                response = func(request_message)
                if isinstance(response, ProtoMessage):
                    return JsonResponse(proto_to_dict(response), status=200, content_type='application/json')
                elif isinstance(response, dict):
                    return JsonResponse(response, status=200, content_type='application/json')
                elif isinstance(response, HttpResponse):
                    return response
            except Exception as e:
                return JsonResponse(error_dict('Error while processing the request', e), status=500,
                                    content_type='application/json')

        return wrapper

    return decorator


def get_proto_schema_validator():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(request: HttpRequest):
            try:
                response = func(request)
                if isinstance(response, ProtoMessage):
                    return JsonResponse(proto_to_dict(response), status=200, content_type='application/json')
                elif isinstance(response, dict):
                    return JsonResponse(response, status=200, content_type='application/json')
                elif isinstance(response, HttpResponse):
                    return response
            except Exception as e:
                return JsonResponse({'message': 'Error while processing the request'}, status=500,
                                    content_type='application/json')

        return wrapper

    return decorator


def api_auth_check(func):
    @functools.wraps(func)
    def _wrapped_view(request, *args, **kwargs):
        request_headers = request.headers
        if 'Authorization' not in request_headers:
            return JsonResponse({'error': 'Authorization header is required'}, status=401)
        bearer, auth_token = request_headers['Authorization'].split(' ')
        if bearer != 'Bearer':
            return JsonResponse({'error': 'Invalid Authorization header'}, status=401)
        drd_proxy_api_token = settings.DRD_CLOUD_API_TOKEN
        if drd_proxy_api_token is None:
            return JsonResponse({'error': 'API token is not set'}, status=401)
        if auth_token != drd_proxy_api_token:
            return JsonResponse({'error': 'Invalid token'}, status=401)
        return func(request, *args, **kwargs)

    return _wrapped_view


def account_post_api(request_schema):
    def decorator(func):
        @functools.wraps(func)
        @csrf_exempt
        @api_view(['POST'])
        @api_auth_check
        @proto_schema_validator(request_schema)
        @log_function_call
        def wrapper(message):
            return func(message)

        return wrapper

    return decorator


def account_get_api():
    def decorator(func):
        @functools.wraps(func)
        @csrf_exempt
        @api_view(['GET'])
        @api_auth_check
        @get_proto_schema_validator()
        @log_function_call
        def wrapper(message):
            return func(message)

        return wrapper

    return decorator
