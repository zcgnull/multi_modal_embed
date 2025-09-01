from flask import (
    Response, jsonify, send_file, make_response,
    request as flask_request,
)
import logging
from functools import wraps

from app import settings

def get_json_result(code=settings.RetCode.SUCCESS, message='success', data=None):
    response = {"code": code, "message": message, "data": data}
    return jsonify(response)

def server_error_response(e):
    logging.exception(e)
    try:
        if e.code == 401:
            return get_json_result(code=401, message=repr(e))
    except BaseException:
        pass
    if len(e.args) > 1:
        return get_json_result(
            code=settings.RetCode.EXCEPTION_ERROR, message=repr(e.args[0]), data=e.args[1])
    if repr(e).find("index_not_found_exception") >= 0:
        return get_json_result(code=settings.RetCode.EXCEPTION_ERROR,
                               message="No chunk found, please upload file and parse it.")

    return get_json_result(code=settings.RetCode.EXCEPTION_ERROR, message=repr(e))

def validate_request(*args, **kwargs):
    def wrapper(func):
        @wraps(func)
        def decorated_function(*_args, **_kwargs):
            input_arguments = flask_request.json or flask_request.form.to_dict()
            no_arguments = []
            error_arguments = []
            for arg in args:
                if arg not in input_arguments:
                    no_arguments.append(arg)
            for k, v in kwargs.items():
                config_value = input_arguments.get(k, None)
                if config_value is None:
                    no_arguments.append(k)
                elif isinstance(v, (tuple, list)):
                    if config_value not in v:
                        error_arguments.append((k, set(v)))
                elif config_value != v:
                    error_arguments.append((k, v))
            if no_arguments or error_arguments:
                error_string = ""
                if no_arguments:
                    error_string += "required argument are missing: {}; ".format(
                        ",".join(no_arguments))
                if error_arguments:
                    error_string += "required argument values: {}".format(
                        ",".join(["{}={}".format(a[0], a[1]) for a in error_arguments]))
                return get_json_result(
                    code=settings.RetCode.ARGUMENT_ERROR, message=error_string)
            return func(*_args, **_kwargs)

        return decorated_function

    return wrapper