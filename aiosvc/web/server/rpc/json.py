import json
import asyncio

from aiohttp import web

from .base import RpcHandler, RpcError


_err_mapping = {
    RpcError.RPC_ERR_INTERNAL_ERROR: (-32603, "Internal error"),
    RpcError.RPC_ERR_PARSE: (-32700, "Parse error"),
    RpcError.RPC_ERR_INVALID_REQUEST: (-32600, "Invalid Request"),
    RpcError.RPC_ERR_METHOD_NOT_FOUND: (-32601, "Method not found"),
    RpcError.RPC_ERR_INVALID_PARAMS_FORMAT: (-32602, "Invalid params"),
    RpcError.RPC_ERR_INVALID_PARAMS: (-32602, "Invalid params"),
}


class JsonRpcHandler(RpcHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # format output(argument will be passed to json.dump function)
        self._json_indent = ' ' * 4
        # process a batch web call as a set of concurrent tasks
        self._concurrent_batch_call = True

    async def _respnse(self, body):
        response = web.json_response(body)
        return response

    async def _handle_request(self, request):
        body = await request.read()

        try:
            data = json.loads(body.decode())
        except json.decoder.JSONDecodeError as e:
            return await self._respnse(self._format_error(RpcError.RPC_ERR_PARSE))

        is_batch = True
        if not isinstance(data, list):
            data = [data]
            is_batch = False

        for req in data:
            if not isinstance(req, dict):
                return await self._respnse(self._format_error(RpcError.RPC_ERR_INVALID_REQUEST))
        results = []
        if self._concurrent_batch_call:
            reqs = []
            for req in data:
                reqs.append(self._exec_req(req, request))
            req_results = await asyncio.gather(*reqs)
            for result in req_results:
                results.append(result)
        else:
            for req in data:
                result = await self._exec_req(req, request)
                results.append(result)

        return await self._respnse(results if is_batch else results[0])

    def _dumps(self, data):
        return json.dumps(data, indent=self._json_indent)

    async def _exec_req(self, req_data, request):
        request_id = None
        try:
            request_id, method_name, method_params = self._parse_call(req_data)
            result = await self._call_method(self, method_name, method_params, request, self._error_on_non_exist_params)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            }
        except Exception as e:
            return self._format_error(e, request_id)

    @staticmethod
    def _format_error(e, request_id=None):
        if not isinstance(e, RpcError):
            e = RpcError(RpcError.RPC_ERR_INTERNAL_ERROR)
        code, message = _err_mapping.get(e.code)
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": message
            },
        }

    def _parse_call(self, req_data):
        request_id = None
        if 'id' in req_data:
            request_id = req_data["id"]
        if 'method' not in req_data or not isinstance(req_data["method"], str):
            raise RpcError(RpcError.RPC_ERR_INVALID_REQUEST,
                               details="method not given or it's not instance of string")
        if 'params' in req_data:
            params = req_data['params']
        else:
            params = {}
        return request_id, req_data["method"], params

    async def _add_routes(self):
        self._http_app.router.add_route("POST", self._route, self._handle_request)
