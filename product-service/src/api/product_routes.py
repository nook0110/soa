from flask import Blueprint, request, jsonify, g
from src.services import ProductService
from src.middleware import logging_middleware, auth_required, role_required
from src.utils.errors import ValidationError

product_bp = Blueprint('products', __name__)

@product_bp.route('/products', methods=['POST'])
@logging_middleware
@role_required('SELLER', 'ADMIN')
def create_product():
    data = request.get_json()
    
    product = ProductService.create_product(
        data=data,
        user_id=g.user_id,
        user_role=g.user_role
    )
    
    return jsonify({
        'id': product.id,
        'name': product.name,
        'description': product.description,
        'price': float(product.price),
        'stock': product.stock,
        'category': product.category,
        'status': product.status.name,
        'seller_id': product.seller_id,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    }), 201

@product_bp.route('/products/<product_id>', methods=['GET'])
@logging_middleware
def get_product(product_id):
    product = ProductService.get_product(product_id)
    
    return jsonify({
        'id': product.id,
        'name': product.name,
        'description': product.description,
        'price': float(product.price),
        'stock': product.stock,
        'category': product.category,
        'status': product.status.name,
        'seller_id': product.seller_id,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    })

@product_bp.route('/products', methods=['GET'])
@logging_middleware
def list_products():
    page = request.args.get('page', 0, type=int)
    size = request.args.get('size', 20, type=int)
    status = request.args.get('status')
    category = request.args.get('category')
    
    if size > 100:
        raise ValidationError({'size': 'Size must not exceed 100'})
    
    result = ProductService.list_products(
        page=page,
        size=size,
        status=status,
        category=category
    )
    
    items = [{
        'id': p.id,
        'name': p.name,
        'description': p.description,
        'price': float(p.price),
        'stock': p.stock,
        'category': p.category,
        'status': p.status.name,
        'seller_id': p.seller_id,
        'created_at': p.created_at.isoformat(),
        'updated_at': p.updated_at.isoformat()
    } for p in result['items']]
    
    return jsonify({
        'items': items,
        'total_elements': result['total_elements'],
        'page': result['page'],
        'size': result['size']
    })

@product_bp.route('/products/<product_id>', methods=['PUT'])
@logging_middleware
@role_required('SELLER', 'ADMIN')
def update_product(product_id):
    data = request.get_json()
    
    product = ProductService.update_product(
        product_id=product_id,
        data=data,
        user_id=g.user_id,
        user_role=g.user_role
    )
    
    return jsonify({
        'id': product.id,
        'name': product.name,
        'description': product.description,
        'price': float(product.price),
        'stock': product.stock,
        'category': product.category,
        'status': product.status.name,
        'seller_id': product.seller_id,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    })

@product_bp.route('/products/<product_id>', methods=['DELETE'])
@logging_middleware
@role_required('SELLER', 'ADMIN')
def delete_product(product_id):
    product = ProductService.delete_product(
        product_id=product_id,
        user_id=g.user_id,
        user_role=g.user_role
    )
    
    return jsonify({
        'id': product.id,
        'name': product.name,
        'description': product.description,
        'price': float(product.price),
        'stock': product.stock,
        'category': product.category,
        'status': product.status.name,
        'seller_id': product.seller_id,
        'created_at': product.created_at.isoformat(),
        'updated_at': product.updated_at.isoformat()
    })