"""Initial schema

Revision ID: 001
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'products',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.String(4000), nullable=True),
        sa.Column('price', sa.Numeric(12, 2), nullable=False),
        sa.Column('stock', sa.Integer, nullable=False),
        sa.Column('category', sa.String(100), nullable=False),
        sa.Column('status', sa.Enum('ACTIVE', 'INACTIVE', 'ARCHIVED', name='product_status'), nullable=False),
        sa.Column('seller_id', sa.String(36), nullable=False),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('updated_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), 
                  onupdate=sa.text('CURRENT_TIMESTAMP'), nullable=False)
    )
    
    op.create_index('idx_products_status', 'products', ['status'])
    op.create_index('idx_products_category', 'products', ['category'])
    op.create_index('idx_products_seller_id', 'products', ['seller_id'])


def downgrade() -> None:
    op.drop_index('idx_products_seller_id', 'products')
    op.drop_index('idx_products_category', 'products')
    op.drop_index('idx_products_status', 'products')
    op.drop_table('products')
    op.execute('DROP TYPE product_status')