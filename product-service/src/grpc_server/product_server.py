import grpc
from concurrent import futures
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../grpc_generated'))

from src.grpc_generated import product_pb2, product_pb2_grpc
from src.services import ProductService, StockService
from src.models import db, init_db
from src.config import Config

class ProductServicer(product_pb2_grpc.ProductServiceServicer):
    def GetProduct(self, request, context):
        try:
            product = ProductService.get_product(request.product_id)
            
            return product_pb2.ProductResponse(
                product_id=product.id,
                name=product.name,
                description=product.description or '',
                price=float(product.price),
                stock=product.stock,
                category=product.category,
                status=product.status.name,
                seller_id=product.seller_id
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            return product_pb2.ProductResponse()
    
    def CheckStock(self, request, context):
        try:
            available = StockService.check_stock(request.product_id, request.quantity)
            
            return product_pb2.StockResponse(
                available=available,
                current_stock=StockService.get_stock(request.product_id)
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return product_pb2.StockResponse(available=False)
    
    def ReserveStock(self, request, context):
        try:
            items = [{'product_id': item.product_id, 'quantity': item.quantity} 
                    for item in request.items]
            
            StockService.reserve_stock(items)
            
            return product_pb2.ReserveStockResponse(success=True)
        except Exception as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(str(e))
            return product_pb2.ReserveStockResponse(success=False, error=str(e))
    
    def ReleaseStock(self, request, context):
        try:
            items = [{'product_id': item.product_id, 'quantity': item.quantity} 
                    for item in request.items]
            
            StockService.release_stock(items)
            
            return product_pb2.ReleaseStockResponse(success=True)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return product_pb2.ReleaseStockResponse(success=False)
    
    def CheckProductActive(self, request, context):
        try:
            is_active = ProductService.check_product_active(request.product_id)
            
            return product_pb2.ProductActiveResponse(is_active=is_active)
        except Exception as e:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(str(e))
            return product_pb2.ProductActiveResponse(is_active=False)

def serve():
    init_db()
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    product_pb2_grpc.add_ProductServiceServicer_to_server(ProductServicer(), server)
    server.add_insecure_port(f'[::]:{Config.GRPC_PORT}')
    
    print(f'Product gRPC server starting on port {Config.GRPC_PORT}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()