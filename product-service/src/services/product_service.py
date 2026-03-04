from typing import Optional, List, Dict
from sqlalchemy import and_
from src.models import db, Product, ProductStatus
from src.utils.errors import ProductNotFoundError, ProductInactiveError, ValidationError, AccessDeniedError

class ProductService:
    @staticmethod
    def create_product(data: dict, user_id: str, user_role: str) -> Product:
        if user_role not in ['SELLER', 'ADMIN']:
            raise AccessDeniedError('Only SELLER and ADMIN can create products')
        
        errors = ProductService._validate_product_data(data)
        if errors:
            raise ValidationError(errors)
        
        product = Product(
            name=data['name'],
            description=data.get('description'),
            price=data['price'],
            stock=data['stock'],
            category=data['category'],
            status=data.get('status', ProductStatus.ACTIVE),
            seller_id=user_id
        )
        
        db.session.add(product)
        db.session.commit()
        db.session.refresh(product)
        
        return product
    
    @staticmethod
    def get_product(product_id: str) -> Product:
        product = db.session.query(Product).filter_by(id=product_id).first()
        
        if not product:
            raise ProductNotFoundError(product_id)
        
        return product
    
    @staticmethod
    def list_products(
        page: int = 0,
        size: int = 20,
        status: Optional[str] = None,
        category: Optional[str] = None
    ) -> Dict:
        query = db.session.query(Product)
        
        if status:
            try:
                status_enum = ProductStatus[status]
                query = query.filter(Product.status == status_enum)
            except KeyError:
                raise ValidationError({'status': f'Invalid status: {status}'})
        
        if category:
            query = query.filter(Product.category == category)
        
        total_elements = query.count()
        
        products = query.offset(page * size).limit(size).all()
        
        return {
            'items': products,
            'total_elements': total_elements,
            'page': page,
            'size': size
        }
    
    @staticmethod
    def update_product(product_id: str, data: dict, user_id: str, user_role: str) -> Product:
        product = ProductService.get_product(product_id)
        
        if user_role == 'SELLER' and product.seller_id != user_id:
            raise AccessDeniedError('You can only update your own products')
        
        errors = ProductService._validate_product_data(data, is_update=True)
        if errors:
            raise ValidationError(errors)
        
        if 'name' in data:
            product.name = data['name']
        if 'description' in data:
            product.description = data['description']
        if 'price' in data:
            product.price = data['price']
        if 'stock' in data:
            product.stock = data['stock']
        if 'category' in data:
            product.category = data['category']
        if 'status' in data:
            product.status = ProductStatus[data['status']]
        
        db.session.commit()
        
        return product
    
    @staticmethod
    def delete_product(product_id: str, user_id: str, user_role: str) -> Product:
        product = ProductService.get_product(product_id)
        
        if user_role == 'SELLER' and product.seller_id != user_id:
            raise AccessDeniedError('You can only delete your own products')
        
        product.status = ProductStatus.ARCHIVED
        db.session.commit()
        
        return product
    
    @staticmethod
    def check_product_active(product_id: str) -> bool:
        product = ProductService.get_product(product_id)
        
        if product.status != ProductStatus.ACTIVE:
            raise ProductInactiveError(product_id)
        
        return True
    
    @staticmethod
    def _validate_product_data(data: dict, is_update: bool = False) -> dict:
        errors = {}
        
        if not is_update or 'name' in data:
            name = data.get('name', '')
            if not name or len(name) < 1:
                errors['name'] = 'Name must be at least 1 character'
            elif len(name) > 255:
                errors['name'] = 'Name must not exceed 255 characters'
        
        if 'description' in data:
            description = data.get('description', '')
            if description and len(description) > 4000:
                errors['description'] = 'Description must not exceed 4000 characters'
        
        if not is_update or 'price' in data:
            price = data.get('price')
            if price is None:
                errors['price'] = 'Price is required'
            elif price <= 0:
                errors['price'] = 'Price must be greater than 0'
        
        if not is_update or 'stock' in data:
            stock = data.get('stock')
            if stock is None:
                errors['stock'] = 'Stock is required'
            elif stock < 0:
                errors['stock'] = 'Stock must be non-negative'
        
        if not is_update or 'category' in data:
            category = data.get('category', '')
            if not category or len(category) < 1:
                errors['category'] = 'Category must be at least 1 character'
            elif len(category) > 100:
                errors['category'] = 'Category must not exceed 100 characters'
        
        if 'status' in data:
            status = data.get('status')
            if status not in [s.name for s in ProductStatus]:
                errors['status'] = f'Invalid status. Must be one of: {", ".join([s.name for s in ProductStatus])}'
        
        return errors