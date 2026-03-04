import grpc
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../grpc_generated'))

from src.grpc_generated import auth_pb2, auth_pb2_grpc
from src.config import Config

class AuthClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(Config.AUTH_GRPC_HOST)
        self.stub = auth_pb2_grpc.AuthServiceStub(self.channel)
    
    def verify_token(self, token: str) -> dict:
        request = auth_pb2.ValidateTokenRequest(token=token)
        
        try:
            response = self.stub.ValidateToken(request)
            
            if not response.valid:
                raise Exception('Token is invalid')
            
            return {
                'user_id': response.user.id,
                'username': response.user.username,
                'email': response.user.email,
                'role': response.user.role
            }
        except grpc.RpcError as e:
            raise Exception(f'Token verification failed: {e.details()}')
    
    def check_permission(self, user_id: str, resource: str, action: str) -> bool:
        request = auth_pb2.CheckPermissionRequest(
            user_id=user_id,
            resource=resource,
            action=action
        )
        
        try:
            response = self.stub.CheckPermission(request)
            return response.allowed
        except grpc.RpcError as e:
            raise Exception(f'Permission check failed: {e.details()}')
    
    def close(self):
        self.channel.close()