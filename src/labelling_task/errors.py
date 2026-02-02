from __future__ import annotations


class AppError(Exception):
    """Base error for expected failures."""

    def __init__(self, message: str, *, http_status: int = 400):
        super().__init__(message)
        self.message = message
        self.http_status = http_status


class AuthError(AppError):
    def __init__(self, message: str = "unauthorized"):
        super().__init__(message, http_status=401)


class ForbiddenError(AppError):
    def __init__(self, message: str = "forbidden"):
        super().__init__(message, http_status=403)


class NotFoundError(AppError):
    def __init__(self, message: str = "not found"):
        super().__init__(message, http_status=404)
