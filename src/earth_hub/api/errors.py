"""One error format for every failure: {"error": {"code", "message", "details"}}."""

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError
from starlette.exceptions import HTTPException as StarletteHTTPException

from earth_hub.api.schemas import ErrorBody, ErrorDetail, ErrorResponse
from earth_hub.consumers.earthquake_consumer import is_infrastructure_error

logger = logging.getLogger(__name__)

ERROR_RESPONSES = {
    404: {"model": ErrorResponse, "description": "Unknown event"},
    422: {"model": ErrorResponse, "description": "Invalid parameters"},
    503: {"model": ErrorResponse, "description": "Database unavailable"},
}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details=None):
        self.status, self.code, self.message = status, code, message
        self.details = details or []

    @classmethod
    def invalid(cls, problems: list[tuple[str, str]]) -> "ApiError":
        return cls(
            422,
            "invalid_parameters",
            "one or more parameters are invalid",
            [ErrorDetail(parameter=p, message=m) for p, m in problems],
        )


def _response(status: int, code: str, message: str, details=None, headers=None) -> JSONResponse:
    body = ErrorResponse(error=ErrorBody(code=code, message=message, details=details or []))
    return JSONResponse(status_code=status, content=body.model_dump(mode="json"), headers=headers)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error(_: Request, exc: ApiError):
        return _response(exc.status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError):
        details = [
            ErrorDetail(
                # loc is e.g. ("query", "limit"); the first part is where, not what.
                parameter=".".join(str(p) for p in err["loc"][1:]) or None,
                message=err["msg"],
            )
            for err in exc.errors()
        ]
        return _response(422, "invalid_parameters", "one or more parameters are invalid", details)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, exc: StarletteHTTPException):
        codes = {404: "not_found", 405: "method_not_allowed"}
        # Keep Starlette's headers, e.g. Allow on a 405, which HTTP requires.
        return _response(
            exc.status_code,
            codes.get(exc.status_code, "http_error"),
            str(exc.detail),
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(DBAPIError)
    async def database_error(request: Request, exc: DBAPIError):
        if is_infrastructure_error(exc):
            logger.error("database unavailable", extra={"path": request.url.path})
            return _response(503, "database_unavailable", "the database is unavailable")
        logger.exception("database error", extra={"path": request.url.path})
        return _response(500, "internal_error", "internal server error")

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception):
        logger.exception("unexpected error", extra={"path": request.url.path})
        # Never return internals (SQL, stack traces) to the client.
        return _response(500, "internal_error", "internal server error")
