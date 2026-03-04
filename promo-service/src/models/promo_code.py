import enum
from datetime import datetime
from sqlalchemy import Column, String, Numeric, Integer, DateTime, Boolean, Enum
from src.models.database import db
import uuid

class DiscountType(enum.Enum):
    PERCENTAGE = 'PERCENTAGE'
    FIXED_AMOUNT = 'FIXED_AMOUNT'

class PromoCode(db.Model):
    __tablename__ = 'promo_codes'
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    code = Column(String(20), unique=True, nullable=False, index=True)
    discount_type = Column(Enum(DiscountType), nullable=False)
    discount_value = Column(Numeric(12, 2), nullable=False)
    min_order_amount = Column(Numeric(12, 2), nullable=False, default=0)
    max_uses = Column(Integer, nullable=False)
    current_uses = Column(Integer, nullable=False, default=0)
    valid_from = Column(DateTime, nullable=False)
    valid_until = Column(DateTime, nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<PromoCode {self.code}>'