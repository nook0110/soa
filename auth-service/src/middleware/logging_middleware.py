import time
import uuid
from flask import request, g
from src.utils.logger import logger

def setup_logging_middleware(app):
    @app.before_request
    def log_request_start():
        g.request_id = request.headers.get('X-Request-Id', str(uuid.uuid4()))
        g.start_time = time.time()
        
        logger.info(
            "request_started",
            request_id=g.request_id,
            method=request.method,
            endpoint=request.path,
            remote_addr=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )

    @app.after_request
    def log_request_end(response):
        duration_ms = (time.time() - g.start_time) * 1000
        
        log_data = {
            "request_id": g.request_id,
            "method": request.method,
            "endpoint": request.path,
            "status_code": response.status_code,
            "duration_ms": round(duration_ms, 2),
            "user_id": g.get('user_id'),
            "remote_addr": request.remote_addr
        }
        
        if request.method in ['POST', 'PUT', 'PATCH', 'DELETE']:
            try:
                body = request.get_json()
                if body and 'password' in body:
                    body = {**body, 'password': '***MASKED***'}
                log_data['request_body'] = body
            except:
                pass
        
        logger.info("request_completed", **log_data)
        
        response.headers['X-Request-Id'] = g.request_id
        return response

    return app