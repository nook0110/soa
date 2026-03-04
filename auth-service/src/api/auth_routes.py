from flask import Blueprint, request, jsonify, g
from marshmallow import Schema, fields, ValidationError, validate
from src.services.user_service import UserService
from src.services.token_service import TokenService
from src.middleware.auth_middleware import require_auth
from src.utils.errors import APIError

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

class RegisterSchema(Schema):
    username = fields.Str(required=True, validate=validate.Length(min=1, max=50))
    email = fields.Email(required=True, validate=validate.Length(max=255))
    password = fields.Str(required=True, validate=validate.Length(min=6, max=100))
    role = fields.Str(validate=validate.OneOf(['USER', 'SELLER', 'ADMIN']), load_default='USER')

class LoginSchema(Schema):
    email = fields.Email(required=True)
    password = fields.Str(required=True)

class RefreshSchema(Schema):
    refresh_token = fields.Str(required=True)

@auth_bp.route('/register', methods=['POST'])
def register():
    try:
        data = RegisterSchema().load(request.get_json())
    except ValidationError as err:
        raise APIError('VALIDATION_ERROR', 'Validation failed', 400, err.messages)
    
    user = UserService.create_user(
        username=data['username'],
        email=data['email'],
        password=data['password'],
        role=data.get('role', 'USER')
    )
    
    access_token = TokenService.generate_access_token(user.id, user.role.value)
    refresh_token = TokenService.generate_refresh_token(user.id)
    
    return jsonify({
        'user': user.to_dict(),
        'access_token': access_token,
        'refresh_token': refresh_token,
        'token_type': 'Bearer',
        'expires_in': 900
    }), 201

@auth_bp.route('/login', methods=['POST'])
def login():
    try:
        data = LoginSchema().load(request.get_json())
    except ValidationError as err:
        raise APIError('VALIDATION_ERROR', 'Validation failed', 400, err.messages)
    
    user = UserService.authenticate_user(data['email'], data['password'])
    
    access_token = TokenService.generate_access_token(user.id, user.role.value)
    refresh_token = TokenService.generate_refresh_token(user.id)
    
    return jsonify({
        'user': user.to_dict(),
        'access_token': access_token,
        'refresh_token': refresh_token,
        'token_type': 'Bearer',
        'expires_in': 900
    }), 200

@auth_bp.route('/refresh', methods=['POST'])
def refresh():
    try:
        data = RefreshSchema().load(request.get_json())
    except ValidationError as err:
        raise APIError('VALIDATION_ERROR', 'Validation failed', 400, err.messages)
    
    payload = TokenService.validate_refresh_token(data['refresh_token'])
    user_id = payload['sub']
    
    user = UserService.get_user_by_id(user_id)
    
    access_token = TokenService.generate_access_token(user.id, user.role.value)
    
    return jsonify({
        'access_token': access_token,
        'token_type': 'Bearer',
        'expires_in': 900
    }), 200

@auth_bp.route('/me', methods=['GET'])
@require_auth
def get_current_user():
    user = UserService.get_user_by_id(g.user_id)
    return jsonify(user.to_dict()), 200

@auth_bp.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'service': 'auth-service'}), 200