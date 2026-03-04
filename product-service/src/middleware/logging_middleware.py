import time
import uuid
from flask import request, g
from functools import wraps
from src.utils.logger import get_logger

logger = get_logger(__name__)

def logging_middleware(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        request_id = str(uuid.uuid4())
        g.request_id = request_id
        
        start_time = time.time()
        
        user_id = getattr(g, 'user_id', None)
        
        try:
            response = f(*args, **kwargs)
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            status_code = response.status_code if hasattr(response, 'status_code') else 200
            
            log_data = {
                'request_id': request_id,
                'method': request.method,
                'endpoint': request.path,
                'status_code': status_code,
                'duration_ms': duration_ms,
                'user_id': user_id,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
            }
            
            if request.method in ['POST', 'PUT', 'DELETE']:
                try:
                    body = request.get_json()
                    if body and 'password' in body:
                        body = {**body, 'password': '***'}
                    log_data['request_body'] = body
                except:
                    pass
            
            logger.info('api_request', **log_data)
            
            if hasattr(response, 'headers'):
                response.headers['X-Request-Id'] = request_id
            
            return response
            
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            
            logger.error('api_request_error',
                request_id=request_id,
                method=request.method,
                endpoint=request.path,
                duration_ms=duration_ms,
                user_id=user_id,
                error=str(e),
                timestamp=time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
            )
            raise
    
    return decorated_function