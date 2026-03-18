CREATE TYPE flight_status AS ENUM ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED');
CREATE TYPE reservation_status AS ENUM ('ACTIVE', 'RELEASED', 'EXPIRED');

CREATE TABLE flights (
    id BIGSERIAL PRIMARY KEY,
    flight_number VARCHAR(20) NOT NULL,
    airline VARCHAR(100) NOT NULL,
    origin_airport VARCHAR(3) NOT NULL,
    destination_airport VARCHAR(3) NOT NULL,
    departure_time TIMESTAMP NOT NULL,
    arrival_time TIMESTAMP NOT NULL,
    total_seats INTEGER NOT NULL CHECK (total_seats > 0),
    available_seats INTEGER NOT NULL CHECK (available_seats >= 0),
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    status flight_status NOT NULL DEFAULT 'SCHEDULED',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT check_seats CHECK (available_seats <= total_seats),
    CONSTRAINT check_times CHECK (arrival_time > departure_time)
);

CREATE INDEX idx_flights_search ON flights(origin_airport, destination_airport, departure_time, status);
CREATE INDEX idx_flights_number_date ON flights(flight_number, departure_time);
CREATE UNIQUE INDEX idx_unique_flight_date ON flights(flight_number, (DATE(departure_time)));

CREATE TABLE seat_reservations (
    id BIGSERIAL PRIMARY KEY,
    flight_id BIGINT NOT NULL REFERENCES flights(id) ON DELETE CASCADE,
    booking_id VARCHAR(100) NOT NULL UNIQUE,
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    status reservation_status NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_reservations_booking ON seat_reservations(booking_id);
CREATE INDEX idx_reservations_flight_status ON seat_reservations(flight_id, status);

INSERT INTO flights (flight_number, airline, origin_airport, destination_airport, departure_time, arrival_time, total_seats, available_seats, price, status)
VALUES
    ('SU1234', 'Aeroflot', 'SVO', 'LED', '2026-04-01 10:00:00', '2026-04-01 11:30:00', 180, 180, 5000.00, 'SCHEDULED'),
    ('SU5678', 'Aeroflot', 'SVO', 'LED', '2026-04-01 14:00:00', '2026-04-01 15:30:00', 180, 150, 5500.00, 'SCHEDULED'),
    ('S71001', 'S7 Airlines', 'DME', 'LED', '2026-04-01 09:00:00', '2026-04-01 10:30:00', 150, 120, 4800.00, 'SCHEDULED'),
    ('UT100', 'UTair', 'VKO', 'LED', '2026-04-01 12:00:00', '2026-04-01 13:30:00', 120, 100, 4500.00, 'SCHEDULED'),
    ('SU2345', 'Aeroflot', 'LED', 'SVO', '2026-04-02 16:00:00', '2026-04-02 17:30:00', 180, 180, 5200.00, 'SCHEDULED');
