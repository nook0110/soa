from sqlalchemy import Column, BigInteger, String, Integer, Numeric, DateTime, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ENUM
import enum

Base = declarative_base()

class BookingStatus(str, enum.Enum):
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"

class Booking(Base):
    __tablename__ = "bookings"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(100), nullable=False)
    flight_id = Column(BigInteger, nullable=False)
    passenger_name = Column(String(200), nullable=False)
    passenger_email = Column(String(200), nullable=False)
    seat_count = Column(Integer, nullable=False)
    total_price = Column(Numeric(10, 2), nullable=False)
    status = Column(ENUM('CONFIRMED', 'CANCELLED', name='booking_status', create_type=False), nullable=False, server_default='CONFIRMED')
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint('seat_count > 0', name='check_seat_count_positive'),
        CheckConstraint('total_price > 0', name='check_total_price_positive'),
    )
