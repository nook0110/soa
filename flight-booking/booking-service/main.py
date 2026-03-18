from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
import grpc
import os
import logging
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

from models import Base, Booking, BookingStatus
from grpc_client import GrpcClient
import flight_service_pb2 as pb
import flight_service_pb2_grpc as pb_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Booking Service")

DATABASE_URL = os.getenv("DATABASE_URL")
FLIGHT_SERVICE_URL = os.getenv("FLIGHT_SERVICE_URL", "localhost:50051")
GRPC_API_KEY = os.getenv("GRPC_API_KEY", "")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

channel = grpc.insecure_channel(FLIGHT_SERVICE_URL)
stub = pb_grpc.FlightServiceStub(channel)
grpc_client = GrpcClient(channel, GRPC_API_KEY)

class CreateBookingRequest(BaseModel):
    user_id: str
    flight_id: int
    passenger_name: str
    passenger_email: EmailStr
    seat_count: int

class BookingResponse(BaseModel):
    id: int
    user_id: str
    flight_id: int
    passenger_name: str
    passenger_email: str
    seat_count: int
    total_price: float
    status: str
    created_at: datetime
    updated_at: datetime

class FlightResponse(BaseModel):
    id: int
    flight_number: str
    airline: str
    origin_airport: str
    destination_airport: str
    departure_time: datetime
    arrival_time: datetime
    total_seats: int
    available_seats: int
    price: float
    status: str

@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def run_migrations():
    from alembic.config import Config
    from alembic import command
    from sqlalchemy import text
    
    try:
        with get_db() as db:
            result = db.execute(text("SELECT version_num FROM alembic_version"))
            current_version = result.scalar()
            if current_version == '001':
                logger.info(f"Database already at version {current_version}, skipping migrations")
                return
    except Exception as e:
        logger.info(f"Alembic version table not found or error: {e}, running migrations")
    
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")
    logger.info("Migrations applied successfully")

@app.on_event("startup")
async def startup_event():
    try:
        run_migrations()
        logger.info("Migrations completed")
        logger.info("Booking Service started successfully")
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        import traceback
        traceback.print_exc()
        raise

@app.get("/")
async def root():
    return {
        "service": "Booking Service",
        "status": "running",
        "circuit_breaker_state": grpc_client.get_circuit_state().value
    }

@app.get("/flights", response_model=List[FlightResponse])
async def search_flights(
    origin: str = Query(..., description="Origin airport IATA code"),
    destination: str = Query(..., description="Destination airport IATA code"),
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format")
):
    try:
        request = pb.SearchFlightsRequest(
            origin_airport=origin,
            destination_airport=destination
        )
        
        if date:
            from google.protobuf.timestamp_pb2 import Timestamp
            dt = datetime.strptime(date, "%Y-%m-%d")
            ts = Timestamp()
            ts.FromDatetime(dt)
            request.date.CopyFrom(ts)
        
        response = grpc_client.call_with_retry(
            stub.SearchFlights,
            request
        )
        
        flights = []
        for flight in response.flights:
            flights.append(FlightResponse(
                id=flight.id,
                flight_number=flight.flight_number,
                airline=flight.airline,
                origin_airport=flight.origin_airport,
                destination_airport=flight.destination_airport,
                departure_time=flight.departure_time.ToDatetime(),
                arrival_time=flight.arrival_time.ToDatetime(),
                total_seats=flight.total_seats,
                available_seats=flight.available_seats,
                price=flight.price,
                status=pb.FlightStatus.Name(flight.status)
            ))
        
        return flights
        
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail="Flight service unavailable")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Search flights error: {e}")
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")

@app.get("/flights/{flight_id}", response_model=FlightResponse)
async def get_flight(flight_id: int):
    try:
        request = pb.GetFlightRequest(flight_id=flight_id)
        response = grpc_client.call_with_retry(
            stub.GetFlight,
            request
        )
        
        flight = response.flight
        return FlightResponse(
            id=flight.id,
            flight_number=flight.flight_number,
            airline=flight.airline,
            origin_airport=flight.origin_airport,
            destination_airport=flight.destination_airport,
            departure_time=flight.departure_time.ToDatetime(),
            arrival_time=flight.arrival_time.ToDatetime(),
            total_seats=flight.total_seats,
            available_seats=flight.available_seats,
            price=flight.price,
            status=pb.FlightStatus.Name(flight.status)
        )
        
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="Flight not found")
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail="Flight service unavailable")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Get flight error: {e}")
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")

@app.post("/bookings", response_model=BookingResponse, status_code=201)
async def create_booking(booking_req: CreateBookingRequest):
    if booking_req.seat_count <= 0:
        raise HTTPException(status_code=400, detail="Seat count must be positive")
    
    try:
        flight_request = pb.GetFlightRequest(flight_id=booking_req.flight_id)
        flight_response = grpc_client.call_with_retry(
            stub.GetFlight,
            flight_request
        )
        flight = flight_response.flight
        
        if flight.status != pb.FlightStatus.FLIGHT_STATUS_SCHEDULED:
            raise HTTPException(status_code=400, detail="Flight is not available for booking")
        
        total_price = Decimal(str(flight.price)) * booking_req.seat_count
        
        with get_db() as db:
            booking = Booking(
                user_id=booking_req.user_id,
                flight_id=booking_req.flight_id,
                passenger_name=booking_req.passenger_name,
                passenger_email=booking_req.passenger_email,
                seat_count=booking_req.seat_count,
                total_price=total_price,
                status=BookingStatus.CONFIRMED
            )
            db.add(booking)
            db.flush()
            
            booking_id = str(booking.id)
            
            try:
                reserve_request = pb.ReserveSeatRequest(
                    flight_id=booking_req.flight_id,
                    seat_count=booking_req.seat_count,
                    booking_id=booking_id
                )
                reserve_response = grpc_client.call_with_retry(
                    stub.ReserveSeats,
                    reserve_request
                )
                
                if not reserve_response.success:
                    db.rollback()
                    raise HTTPException(status_code=400, detail=reserve_response.message)
                
                db.commit()
                db.refresh(booking)
                
                return BookingResponse(
                    id=booking.id,
                    user_id=booking.user_id,
                    flight_id=booking.flight_id,
                    passenger_name=booking.passenger_name,
                    passenger_email=booking.passenger_email,
                    seat_count=booking.seat_count,
                    total_price=float(booking.total_price),
                    status=booking.status,
                    created_at=booking.created_at,
                    updated_at=booking.updated_at
                )
                
            except grpc.RpcError as e:
                db.rollback()
                if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                    raise HTTPException(status_code=409, detail="Not enough seats available")
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    raise HTTPException(status_code=503, detail="Flight service unavailable")
                raise HTTPException(status_code=500, detail=str(e))
            except Exception as e:
                db.rollback()
                raise
                
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="Flight not found")
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail="Flight service unavailable")
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create booking error: {e}")
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")

@app.get("/bookings/{booking_id}", response_model=BookingResponse)
async def get_booking(booking_id: int):
    with get_db() as db:
        booking = db.query(Booking).filter(Booking.id == booking_id).first()
        if not booking:
            raise HTTPException(status_code=404, detail="Booking not found")
        
        return BookingResponse(
            id=booking.id,
            user_id=booking.user_id,
            flight_id=booking.flight_id,
            passenger_name=booking.passenger_name,
            passenger_email=booking.passenger_email,
            seat_count=booking.seat_count,
            total_price=float(booking.total_price),
            status=booking.status,
            created_at=booking.created_at,
            updated_at=booking.updated_at
        )

@app.post("/bookings/{booking_id}/cancel", response_model=BookingResponse)
async def cancel_booking(booking_id: int):
    with get_db() as db:
        booking = db.query(Booking).filter(Booking.id == booking_id).first()
        if not booking:
            raise HTTPException(status_code=404, detail="Booking not found")
        
        if booking.status != BookingStatus.CONFIRMED:
            raise HTTPException(status_code=400, detail="Booking is not in CONFIRMED status")
        
        try:
            release_request = pb.ReleaseReservationRequest(booking_id=str(booking_id))
            release_response = grpc_client.call_with_retry(
                stub.ReleaseReservation,
                release_request
            )
            
            if not release_response.success:
                logger.warning(f"Failed to release reservation: {release_response.message}")
            
        except grpc.RpcError as e:
            logger.warning(f"Failed to release seats via gRPC: {e}")
        except Exception as e:
            logger.warning(f"Error releasing reservation: {e}")
        
        booking.status = BookingStatus.CANCELLED
        db.commit()
        db.refresh(booking)
        
        return BookingResponse(
            id=booking.id,
            user_id=booking.user_id,
            flight_id=booking.flight_id,
            passenger_name=booking.passenger_name,
            passenger_email=booking.passenger_email,
            seat_count=booking.seat_count,
            total_price=float(booking.total_price),
            status=booking.status,
            created_at=booking.created_at,
            updated_at=booking.updated_at
        )

@app.get("/bookings", response_model=List[BookingResponse])
async def list_bookings(user_id: str = Query(..., description="User ID")):
    with get_db() as db:
        bookings = db.query(Booking).filter(Booking.user_id == user_id).all()
        
        return [
            BookingResponse(
                id=booking.id,
                user_id=booking.user_id,
                flight_id=booking.flight_id,
                passenger_name=booking.passenger_name,
                passenger_email=booking.passenger_email,
                seat_count=booking.seat_count,
                total_price=float(booking.total_price),
                status=booking.status,
                created_at=booking.created_at,
                updated_at=booking.updated_at
            )
            for booking in bookings
        ]

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("HTTP_PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
