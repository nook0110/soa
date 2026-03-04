import jwt
import hashlib
from datetime import datetime, timedelta
from src.config import Config
from src.utils.errors import APIError
from src.models.refresh_token import RefreshToken
from src.models.database import SessionLocal

class TokenService:
    @staticmethod
    def generate_access_token(user_id, role):
        payload = {
            'sub': str(user_id),
            'role': role,
            'exp': datetime.utcnow() + timedelta(seconds=Config.JWT_ACCESS_TOKEN_EXPIRES),
            'iat': datetime.utcnow(),
            'type': 'access'
        }
        return jwt.encode(payload, Config.JWT_SECRET_KEY, algorithm='HS256')

    @staticmethod
    def generate_refresh_token(user_id):
        payload = {
            'sub': str(user_id),
            'exp': datetime.utcnow() + timedelta(seconds=Config.JWT_REFRESH_TOKEN_EXPIRES),
            'iat': datetime.utcnow(),
            'type': 'refresh'
        }
        token = jwt.encode(payload, Config.JWT_SECRET_KEY, algorithm='HS256')
        
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        
        db = SessionLocal()
        try:
            refresh_token = RefreshToken(
                user_id=user_id,
                token_hash=token_hash,
                expires_at=datetime.utcnow() + timedelta(seconds=Config.JWT_REFRESH_TOKEN_EXPIRES)
            )
            db.add(refresh_token)
            db.commit()
        finally:
            db.close()
        
        return token

    @staticmethod
    def validate_access_token(token):
        try:
            payload = jwt.decode(token, Config.JWT_SECRET_KEY, algorithms=['HS256'])
            
            if payload.get('type') != 'access':
                raise APIError('TOKEN_INVALID', 'Invalid token type', 401)
            
            return payload
        except jwt.ExpiredSignatureError:
            raise APIError('TOKEN_EXPIRED', 'Token has expired', 401)
        except jwt.InvalidTokenError:
            raise APIError('TOKEN_INVALID', 'Invalid token', 401)

    @staticmethod
    def validate_refresh_token(token):
        try:
            payload = jwt.decode(token, Config.JWT_SECRET_KEY, algorithms=['HS256'])
            
            if payload.get('type') != 'refresh':
                raise APIError('REFRESH_TOKEN_INVALID', 'Invalid token type', 401)
            
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            
            db = SessionLocal()
            try:
                refresh_token = db.query(RefreshToken).filter_by(token_hash=token_hash).first()
                
                if not refresh_token or not refresh_token.is_valid():
                    raise APIError('REFRESH_TOKEN_INVALID', 'Invalid or expired refresh token', 401)
                
                return payload
            finally:
                db.close()
                
        except jwt.ExpiredSignatureError:
            raise APIError('REFRESH_TOKEN_INVALID', 'Refresh token has expired', 401)
        except jwt.InvalidTokenError:
            raise APIError('REFRESH_TOKEN_INVALID', 'Invalid refresh token', 401)

    @staticmethod
    def revoke_refresh_token(token):
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        
        db = SessionLocal()
        try:
            refresh_token = db.query(RefreshToken).filter_by(token_hash=token_hash).first()
            if refresh_token:
                refresh_token.revoked = True
                db.commit()
        finally:
            db.close()