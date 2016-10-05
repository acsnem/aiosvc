import types
import aiosvc
from aiosvc.web.server import SimpleHandler
from aiohttp import web

class RpcHandler(SimpleHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # raise exception if caught unexpected parameter
        self._error_on_non_exist_params = True
        # logging: 0 - disabled, 1 - errors only, 2 - all requests
        self._logging = 0

    async def _before_call(self, method, params, request):
        return method, params

    async def _handle_request(self, request):
        return web.Response(body=b'')

    async def _log(self, log_data):
        import json
        # print(json.dumps(log_data, indent='    '))
        pass

    def _get_request_log_data(self, request, body):
        if self._logging > 0:
            return {
                "request_body": self._utf_decode(body),
                "request_headers": "\n".join(
                    [self._utf_decode(line[0]) + ": " + self._utf_decode(line[1]) for line in request.raw_headers]),
                "request_uri": request.path
            }

    @staticmethod
    def _utf_decode(bytes):
        try:
            if isinstance(bytes, bytearray):
                return bytes.decode("UTF-8")
            return bytes.decode("UTF-8")
        except:
            return str(bytes)

    @staticmethod
    async def _call_method(obj, method_name, params, request, error_on_non_exist_params):
        method = RpcHandler._get_method(obj, method_name)
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise RpcError(RpcError.RPC_ERR_INVALID_PARAMS_FORMAT, details="params is not instance of dict")
        kwargs = RpcHandler._get_params(method, params, error_on_non_exist_params)
        if hasattr(obj, '_before_call'):
            method, kwargs = await obj._before_call(method, kwargs, request)
        result = await method(**kwargs)
        return result

    @staticmethod
    def _get_method(obj, method_name):
        # private methods
        if method_name[0:1] == "_":
            raise RpcError(RpcError.RPC_ERR_METHOD_NOT_FOUND,
                           details='attempt to call private method "%s"' % method_name)
        try:
            method = getattr(obj, method_name)
        except AttributeError:
            raise RpcError(RpcError.RPC_ERR_METHOD_NOT_FOUND,
                           details='method "%s" not found' % method_name)
        if not isinstance(method, types.MethodType) and not isinstance(method, types.FunctionType):
            raise RpcError(RpcError.RPC_ERR_METHOD_NOT_FOUND,
                           details='method "%s" is not callable' % method_name)
        return method

    @staticmethod
    def _get_params(method, called_params, error_on_non_exist_params):
        kwargs = {}
        varcount = method.__code__.co_argcount + method.__code__.co_kwonlyargcount
        varnames = list(method.__code__.co_varnames)[0:varcount]

        defaults = method.__defaults__
        defautts_pos = len(varnames) - len(defaults or ())
        # remove "self"(first var)
        if isinstance(method, types.MethodType):
            varnames.pop(0)
            defautts_pos -= 1
        for pos in range(len(varnames)):
            kwarg = varnames[pos]
            try:
                kwargs[kwarg] = called_params.pop(kwarg)
            except KeyError:
                if pos >= defautts_pos:
                    kwargs[kwarg] = defaults[pos - defautts_pos]
                else:
                    print(pos, kwarg, defautts_pos, varnames)
                    raise RpcError(RpcError.RPC_ERR_INVALID_PARAMS, details='parameter "%s" not given' % kwarg)
        if error_on_non_exist_params and len(called_params) > 0:
            raise RpcError(RpcError.RPC_ERR_INVALID_PARAMS,
                           details='got unexpected parameter(s): %s' % (", ".join(called_params)))
        return kwargs


class RpcError(Exception):

    RPC_ERR_PARSE = 1
    RPC_ERR_INVALID_REQUEST = 2
    RPC_ERR_METHOD_NOT_FOUND = 3
    RPC_ERR_INVALID_PARAMS_FORMAT = 4
    RPC_ERR_INVALID_PARAMS = 4
    RPC_ERR_INTERNAL_ERROR = 5

    def __init__(self, code, message=None, details=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = code
        self.message = message or self.get_message(code)
        self.details = details

    def __str__(self):
        details = ('\n' + str(self.details)) if self.details is not None else ''
        return 'JsonRpcError(%s): %s%s' % (self.code, self.message, details)

    @staticmethod
    def get_message(code):
        return 'Unknown error'
