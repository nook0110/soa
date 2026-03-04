import bcrypt
from sqlalchemy.exc import IntegrityError
from src.models.user import User, UserRole
from src.models.database import SessionLocal
from src.utils.errors import APIError

class UserService:
    @staticmethod
    def hash_password(password):
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    @staticmethod
    def verify_password(password, password_hash):
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

    @staticmethod
    def create_user(username, email, password, role='USER'):
        db = SessionLocal()
        try:
            if role not in [r.value for r in UserRole]:
                raise APIError('VALIDATION_ERROR', f'Invalid role: {role}', 400)
            
            existing_user = db.query(User).filter(
                (User.username == username) | (User.email == email)
            ).first()
            
            if existing_user:
                if existing_user.username == username:
                    raise APIError('USER_ALREADY_EXISTS', 'Username already exists', 409)
                else:
                    raise APIError('USER_ALREADY_EXISTS', 'Email already exists', 409)
            
            password_hash = UserService.hash_password(password)
            
            user = User(
                username=username,
                email=email,
                password_hash=password_hash,
                role=UserRole[role]
            )
            
            db.add(user)
            db.commit()
            db.refresh(user)
            
            return user
        except IntegrityError:
            db.rollback()
            raise APIError('USER_ALREADY_EXISTS', 'User already exists', 409)
        finally:
            db.close()

    @staticmethod
    def authenticate_user(email, password):
        db = SessionLocal()
        try:
            user = db.query(User).filter_by(email=email).first()
            
            if not user:
                raise APIError('INVALID_CREDENTIALS', 'Invalid email or password', 401)
            
            if not user.is_active:
                raise APIError('ACCESS_DENIED', 'User account is inactive', 403)
            
            if not UserService.verify_password(password, user.password_hash):
                raise APIError('INVALID_CREDENTIALS', 'Invalid email or password', 401)
            
            return user
        finally:
            db.close()

    @staticmethod
    def get_user_by_id(user_id):
        db = SessionLocal()
        try:
            user = db.query(User).filter_by(id=user_id).first()
            
            if not user:
                raise APIError('USER_NOT_FOUND', 'User not found', 404)
            
            return user
        finally:
            db.close()

    @staticmethod
    def get_user_by_email(email):
        db = SessionLocal()
        try:
            user = db.query(User).filter_by(email=email).first()
            return user
        finally:
            db.close()

    @staticmethod
    def check_permission(user_id, resource, action):
        user = UserService.get_user_by_id(user_id)
        
        if user.role == UserRole.ADMIN:
            return True
        
        permissions = {
            'products': {
                'create': [UserRole.SELLER, UserRole.ADMIN],
                'update': [UserRole.SELLER, UserRole.ADMIN],
                'delete': [UserRole.SELLER, UserRole.ADMIN],
                'read': [UserRole.USER, UserRole.SELLER, UserRole.ADMIN]
            },
            'orders': {
                'create': [UserRole.USER, UserRole.ADMIN],
                'read': [UserRole.USER, UserRole.ADMIN],
                'update': [UserRole.USER, UserRole.ADMIN],
                'cancel': [UserRole.USER, UserRole.ADMIN]
            },
            'promo_codes': {
                'create': [UserRole.SELLER, UserRole.ADMIN],
                'read': [UserRole.USER, UserRole.SELLER, UserRole.ADMIN],
                'update': [UserRole.SELLER, UserRole.ADMIN],
                'delete': [UserRole.SELLER, UserRole.ADMIN]
            }
        }
        
        if resource in permissions and action in permissions[resource]:
            return user.role in permissions[resource][action]
        
        return False