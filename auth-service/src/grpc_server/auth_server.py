import grpc
from concurrent import futures
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../grpc_generated'))

from src.grpc_generated import auth_pb2, auth_pb2_grpc, common_pb2
from src.services.user_service import UserService
from src.services.token_service import TokenService
from src.utils.errors import APIError
from src.utils.logger import logger

class AuthServicer(auth_pb2_grpc.AuthServiceServicer):
    def ValidateToken(self, request, context):
        try:
            payload = TokenService.validate_access_token(request.token)
            
            user = UserService.get_user_by_id(payload['sub'])
            
            return auth_pb2.ValidateTokenResponse(
                valid=True,
                user=auth_pb2.UserInfo(
                    id=str(user.id),
                    username=user.username,
                    email=user.email,
                    role=user.role.value,
                    is_active=user.is_active
                )
            )
        except APIError as e:
            return auth_pb2.ValidateTokenResponse(
                valid=False,
                error=common_pb2.Error(
                    error_code=e.error_code,
                    message=e.message,
                    details=e.details
                )
            )
        except Exception as e:
            logger.error("validate_token_error", error=str(e))
            return auth_pb2.ValidateTokenResponse(
                valid=False,
                error=common_pb2.Error(
                    error_code='INTERNAL_ERROR',
                    message='Internal server error'
                )
            )

    def GetUserById(self, request, context):
        try:
            user = UserService.get_user_by_id(request.user_id)
            
            return auth_pb2.UserInfo(
                id=str(user.id),
                username=user.username,
                email=user.email,
                role=user.role.value,
                is_active=user.is_active
            )
        except APIError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND if e.status_code == 404 else grpc.StatusCode.INTERNAL)
            context.set_details(e.message)
            return auth_pb2.UserInfo()
        except Exception as e:
            logger.error("get_user_by_id_error", error=str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Internal server error')
            return auth_pb2.UserInfo()

    def CheckPermission(self, request, context):
        try:
            allowed = UserService.check_permission(
                request.user_id,
                request.resource,
                request.action
            )
            
            return auth_pb2.CheckPermissionResponse(allowed=allowed)
        except Exception as e:
            logger.error("check_permission_error", error=str(e))
            return auth_pb2.CheckPermissionResponse(allowed=False)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    auth_pb2_grpc.add_AuthServiceServicer_to_server(AuthServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"gRPC server started on port {port}")
    return server