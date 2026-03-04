from flask import jsonify

class APIError(Exception):
    def __init__(self, error_code, message, status_code=400, details=None):
        self.error_code = error_code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self):
        return {
            'error_code': self.error_code,
            'message': self.message,
            'details': self.details
        }

def register_error_handlers(app):
    @app.errorhandler(APIError)
    def handle_api_error(error):
        response = error.to_dict()
        return jsonify(response), error.status_code

    @app.errorhandler(404)
    def handle_not_found(error):
        return jsonify({
            'error_code': 'NOT_FOUND',
            'message': 'Resource not found',
            'details': {}
        }), 404

    @app.errorhandler(500)
    def handle_internal_error(error):
        return jsonify({
            'error_code': 'INTERNAL_ERROR',
            'message': 'Internal server error',
            'details': {}
        }), 500

ERROR_CODES = {
    'VALIDATION_ERROR': (400, 'Validation error'),
    'TOKEN_INVALID': (401, 'Invalid token'),
    'TOKEN_EXPIRED': (401, 'Token expired'),
    'REFRESH_TOKEN_INVALID': (401, 'Invalid refresh token'),
    'ACCESS_DENIED': (403, 'Access denied'),
    'USER_NOT_FOUND': (404, 'User not found'),
    'USER_ALREADY_EXISTS': (409, 'User already exists'),
    'INVALID_CREDENTIALS': (401, 'Invalid credentials'),
}