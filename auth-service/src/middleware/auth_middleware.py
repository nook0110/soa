from functools import wraps
from flask import request, g
from src.services.token_service import TokenService
from src.utils.errors import APIError

def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            raise APIError('TOKEN_INVALID', 'Authorization header missing', 401)
        
        parts = auth_header.split()
        
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            raise APIError('TOKEN_INVALID', 'Invalid authorization header format', 401)
        
        token = parts[1]
        
        payload = TokenService.validate_access_token(token)
        
        g.user_id = payload['sub']
        g.user_role = payload['role']
        
        return f(*args, **kwargs)
    
    return decorated_function

def require_role(*allowed_roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not hasattr(g, 'user_role'):
                raise APIError('TOKEN_INVALID', 'Authentication required', 401)
            
            if g.user_role not in allowed_roles:
                raise APIError('ACCESS_DENIED', 'Insufficient permissions', 403)
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator