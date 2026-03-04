from typing import List, Dict
from src.models import db, Product
from src.utils.errors import ProductNotFoundError, InsufficientStockError

class StockService:
    @staticmethod
    def check_stock(product_id: str, quantity: int) -> bool:
        product = db.session.query(Product).filter_by(id=product_id).first()
        
        if not product:
            raise ProductNotFoundError(product_id)
        
        return product.stock >= quantity
    
    @staticmethod
    def reserve_stock(items: List[Dict[str, any]]) -> None:
        insufficient_items = []
        
        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']
            
            product = db.session.query(Product).filter_by(id=product_id).first()
            
            if not product:
                raise ProductNotFoundError(product_id)
            
            if product.stock < quantity:
                insufficient_items.append({
                    'product_id': product_id,
                    'requested': quantity,
                    'available': product.stock
                })
        
        if insufficient_items:
            raise InsufficientStockError({'items': insufficient_items})
        
        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']
            
            product = db.session.query(Product).filter_by(id=product_id).first()
            product.stock -= quantity
        
        db.session.commit()
    
    @staticmethod
    def release_stock(items: List[Dict[str, any]]) -> None:
        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']
            
            product = db.session.query(Product).filter_by(id=product_id).first()
            
            if product:
                product.stock += quantity
        
        db.session.commit()
    
    @staticmethod
    def get_stock(product_id: str) -> int:
        product = db.session.query(Product).filter_by(id=product_id).first()
        
        if not product:
            raise ProductNotFoundError(product_id)
        
        return product.stock