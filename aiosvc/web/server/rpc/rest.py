import json
import urllib.parse

from aiohttp import web

from .base import RpcHandler


class RestRpcHandler(RpcHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # format output(argument will be passed to json.dump function)
        self._json_indent = ' ' * 4

    async def _handle_request(self, request):
        log_data = {}
        try:
            method_name = request.method.lower() + '_' + request.path[len(self._route):].lstrip('/').replace('/', '_')
            print('method_name', method_name)
            query_args = urllib.parse.parse_qs(request.query_string)
            print('query_args', query_args)
            method_params = {key: query_args[key].pop(0) for key in query_args}
            print('method_params', method_params)

            result = await self._call_method(self, method_name, method_params, request, self._error_on_non_exist_params)

            return await self._respnse(result)
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            return await self._respnse(self._format_error(e))

    @staticmethod
    def _format_error(e):
        return {
            "error": {
                "code": 500,
                "message": "Error",
                "exception": str(e)
            },
        }

    async def _respnse(self, body):
        response = web.json_response(body)
        return response

    def _dumps(self, data):
        return json.dumps(data, indent=self._json_indent)

    async def _add_routes(self):
        methods = [d.split('_', 1) for d in self.__dir__() if d[0:1] != '_' and '_' in d]
        print(methods)
        for http_method, rpc_method in methods:
            if http_method.upper() in {'POST', 'PUT', 'DELETE', 'TRACE', 'CONNECT', 'GET', 'HEAD', 'PATCH', 'OPTIONS'}:
                print(http_method.upper(), self._route + rpc_method.replace('_', '/'))
                self._http_app.router.add_route(http_method.upper(), self._route + rpc_method.replace('_', '/'), self._handle_request)

