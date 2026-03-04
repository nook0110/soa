from flask import request, g
from functools import wraps
from src.grpc_clients.auth_client import AuthClient
from src.utils.errors import TokenInvalidError, TokenExpiredError, AccessDeniedError

auth_client = AuthClient()

def auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if not auth_header or not auth_header.startswith('Bearer '):
            raise TokenInvalidError()
        
        token = auth_header.split(' ')[1]
        
        try:
            user_data = auth_client.verify_token(token)
            g.user_id = user_data['user_id']
            g.user_role = user_data['role']
        except Exception as e:
            error_msg = str(e)
            if 'expired' in error_msg.lower():
                raise TokenExpiredError()
            else:
                raise TokenInvalidError()
        
        return f(*args, **kwargs)
    
    return decorated_function

def role_required(*allowed_roles):
    def decorator(f):
        @wraps(f)
        @auth_required
        def decorated_function(*args, **kwargs):
            user_role = getattr(g, 'user_role', None)
            
            if user_role not in allowed_roles:
                raise AccessDeniedError(f'Required role: {", ".join(allowed_roles)}')
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator