"""init schema

Revision ID: 001
Revises: 
Create Date: 2026-03-18 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    result = conn.execute(sa.text(
        "SELECT 1 FROM pg_type WHERE typname = 'booking_status'"
    ))
    if not result.fetchone():
        op.execute("CREATE TYPE booking_status AS ENUM ('CONFIRMED', 'CANCELLED')")
    
    op.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            flight_id BIGINT NOT NULL,
            passenger_name VARCHAR(200) NOT NULL,
            passenger_email VARCHAR(200) NOT NULL,
            seat_count INTEGER NOT NULL CHECK (seat_count > 0),
            total_price NUMERIC(10, 2) NOT NULL CHECK (total_price > 0),
            status booking_status NOT NULL DEFAULT 'CONFIRMED',
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    
    op.execute("CREATE INDEX IF NOT EXISTS idx_bookings_user_status ON bookings(user_id, status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_bookings_flight ON bookings(flight_id)")


def downgrade() -> None:
    op.execute('DROP INDEX IF EXISTS idx_bookings_flight')
    op.execute('DROP INDEX IF EXISTS idx_bookings_user_status')
    op.execute('DROP TABLE IF EXISTS bookings')
    op.execute('DROP TYPE IF EXISTS booking_status')
