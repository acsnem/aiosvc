import json
import asyncio

from aiohttp import web

from .base import RpcHandler, RpcError


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
            return await self._respnse(self._format_error(JsonRpcError.ERR_PARSE))

        is_batch = True
        if not isinstance(data, list):
            data = [data]
            is_batch = False

        for req in data:
            if not isinstance(req, dict):
                return await self._respnse(self._format_error(JsonRpcError.ERR_INVALID_REQUEST))
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
        except RpcError as e:
            code = JsonRpcError.get_protocol_code(e)
            return self._format_error(code, request_id)

    @staticmethod
    def _format_error(code, request_id=None):
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": JsonRpcError.get_message(code)
            },
        }

    def _parse_call(self, req_data):
        request_id = None
        if 'id' in req_data:
            request_id = req_data["id"]
        if 'method' not in req_data or not isinstance(req_data["method"], str):
            raise JsonRpcError(JsonRpcError.ERR_INVALID_REQUEST,
                               details="method not given or it's not instance of string")
        if 'params' in req_data:
            params = req_data['params']
        else:
            params = {}
        return request_id, req_data["method"], params

    async def _add_routes(self):
        self._http_app.router.add_route("POST", self._route, self._handle_request)


class JsonRpcError(RpcError):

    # Invalid JSON was received by the server. An error occurred on the server while parsing the JSON text.
    ERR_PARSE = -32700
    # The JSON sent is not a valid Request object.
    ERR_INVALID_REQUEST = -32600
    # The method does not exist / is not available.
    ERR_METHOD_NOT_FOUND = 32601
    # Invalid method parameter(s).
    ERR_INVALID_PARAMS = -32602
    # Internal JSON-RPC error.
    ERR_INTERNAL_ERROR = -32603
    # -32000 to -32099	Server error	Reserved for implementation-defined server-errors.

    err_mapping = {
        RpcError.RPC_ERR_INTERNAL_ERROR: ERR_INTERNAL_ERROR,
        RpcError.RPC_ERR_PARSE: ERR_PARSE,
        RpcError.RPC_ERR_INVALID_REQUEST: ERR_INVALID_REQUEST,
        RpcError.RPC_ERR_METHOD_NOT_FOUND: ERR_METHOD_NOT_FOUND,
        RpcError.RPC_ERR_INVALID_PARAMS_FORMAT: ERR_INVALID_REQUEST,
        RpcError.RPC_ERR_INVALID_PARAMS: ERR_INVALID_PARAMS,
    }

    @staticmethod
    def get_protocol_code(e):

        if isinstance(e, JsonRpcError):
            return e.code
        elif isinstance(e, RpcError):
            return JsonRpcError.err_mapping.get(e.code)
        else:
            return JsonRpcError.ERR_INTERNAL_ERROR

    @staticmethod
    def get_message(code):
        return {
            JsonRpcError.ERR_PARSE: "Parse error",
            JsonRpcError.ERR_INVALID_REQUEST: "Invalid Request  ",
            JsonRpcError.ERR_METHOD_NOT_FOUND: "Method not found",
            JsonRpcError.ERR_INVALID_PARAMS: "Invalid params",
            JsonRpcError.ERR_INTERNAL_ERROR: "Internal error",
        }.get(code, "Server error")
