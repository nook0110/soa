from flask import jsonify

class APIError(Exception):
    def __init__(self, error_code: str, message: str, status_code: int = 400, details: dict = None):
        self.error_code = error_code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self):
        error_dict = {
            'error_code': self.error_code,
            'message': self.message
        }
        if self.details:
            error_dict['details'] = self.details
        return error_dict

class ProductNotFoundError(APIError):
    def __init__(self, product_id: str):
        super().__init__(
            error_code='PRODUCT_NOT_FOUND',
            message=f'Product with id {product_id} not found',
            status_code=404
        )

class ProductInactiveError(APIError):
    def __init__(self, product_id: str):
        super().__init__(
            error_code='PRODUCT_INACTIVE',
            message=f'Product with id {product_id} is not active',
            status_code=409
        )

class InsufficientStockError(APIError):
    def __init__(self, details: dict):
        super().__init__(
            error_code='INSUFFICIENT_STOCK',
            message='Insufficient stock for requested products',
            status_code=409,
            details=details
        )

class ValidationError(APIError):
    def __init__(self, details: dict):
        super().__init__(
            error_code='VALIDATION_ERROR',
            message='Validation failed',
            status_code=400,
            details=details
        )

class AccessDeniedError(APIError):
    def __init__(self, message: str = 'Access denied'):
        super().__init__(
            error_code='ACCESS_DENIED',
            message=message,
            status_code=403
        )

class TokenExpiredError(APIError):
    def __init__(self):
        super().__init__(
            error_code='TOKEN_EXPIRED',
            message='Access token has expired',
            status_code=401
        )

class TokenInvalidError(APIError):
    def __init__(self):
        super().__init__(
            error_code='TOKEN_INVALID',
            message='Access token is invalid',
            status_code=401
        )

def register_error_handlers(app):
    @app.errorhandler(APIError)
    def handle_api_error(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    @app.errorhandler(404)
    def handle_not_found(error):
        return jsonify({
            'error_code': 'NOT_FOUND',
            'message': 'Resource not found'
        }), 404

    @app.errorhandler(500)
    def handle_internal_error(error):
        return jsonify({
            'error_code': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500