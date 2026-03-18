package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "flight-service/proto/flight"
)

type server struct {
	pb.UnimplementedFlightServiceServer
	db         *sql.DB
	redis      *redis.Client
	cacheTTL   time.Duration
	apiKey     string
}

type Flight struct {
	ID                 int64
	FlightNumber       string
	Airline            string
	OriginAirport      string
	DestinationAirport string
	DepartureTime      time.Time
	ArrivalTime        time.Time
	TotalSeats         int32
	AvailableSeats     int32
	Price              float64
	Status             string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func authInterceptor(apiKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		keys := md.Get("api-key")
		if len(keys) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing api-key")
		}

		if keys[0] != apiKey {
			return nil, status.Error(codes.Unauthenticated, "invalid api-key")
		}

		return handler(ctx, req)
	}
}

func (s *server) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	log.Printf("SearchFlights: origin=%s, destination=%s", req.OriginAirport, req.DestinationAirport)

	cacheKey := fmt.Sprintf("search:%s:%s", req.OriginAirport, req.DestinationAirport)
	if req.Date != nil {
		dateStr := req.Date.AsTime().Format("2006-01-02")
		cacheKey = fmt.Sprintf("%s:%s", cacheKey, dateStr)
	}

	cached, err := s.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		log.Printf("Cache HIT for key: %s", cacheKey)
		var flights []*pb.Flight
		if err := json.Unmarshal([]byte(cached), &flights); err == nil {
			return &pb.SearchFlightsResponse{Flights: flights}, nil
		}
	} else {
		log.Printf("Cache MISS for key: %s", cacheKey)
	}

	query := `
		SELECT id, flight_number, airline, origin_airport, destination_airport,
		       departure_time, arrival_time, total_seats, available_seats, price,
		       status, created_at, updated_at
		FROM flights
		WHERE origin_airport = $1 AND destination_airport = $2 AND status = 'SCHEDULED'
	`
	args := []interface{}{req.OriginAirport, req.DestinationAirport}

	if req.Date != nil {
		query += " AND DATE(departure_time) = $3"
		args = append(args, req.Date.AsTime().Format("2006-01-02"))
	}

	query += " ORDER BY departure_time"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}
	defer rows.Close()

	var flights []*pb.Flight
	for rows.Next() {
		var f Flight
		err := rows.Scan(&f.ID, &f.FlightNumber, &f.Airline, &f.OriginAirport, &f.DestinationAirport,
			&f.DepartureTime, &f.ArrivalTime, &f.TotalSeats, &f.AvailableSeats, &f.Price,
			&f.Status, &f.CreatedAt, &f.UpdatedAt)
		if err != nil {
			return nil, status.Error(codes.Internal, "scan error")
		}

		flights = append(flights, &pb.Flight{
			Id:                 f.ID,
			FlightNumber:       f.FlightNumber,
			Airline:            f.Airline,
			OriginAirport:      f.OriginAirport,
			DestinationAirport: f.DestinationAirport,
			DepartureTime:      timestamppb.New(f.DepartureTime),
			ArrivalTime:        timestamppb.New(f.ArrivalTime),
			TotalSeats:         f.TotalSeats,
			AvailableSeats:     f.AvailableSeats,
			Price:              f.Price,
			Status:             mapFlightStatus(f.Status),
			CreatedAt:          timestamppb.New(f.CreatedAt),
			UpdatedAt:          timestamppb.New(f.UpdatedAt),
		})
	}

	if len(flights) > 0 {
		data, _ := json.Marshal(flights)
		s.redis.Set(ctx, cacheKey, data, s.cacheTTL)
	}

	return &pb.SearchFlightsResponse{Flights: flights}, nil
}

func (s *server) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.GetFlightResponse, error) {
	log.Printf("GetFlight: id=%d", req.FlightId)

	cacheKey := fmt.Sprintf("flight:%d", req.FlightId)

	cached, err := s.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		log.Printf("Cache HIT for key: %s", cacheKey)
		var flight pb.Flight
		if err := json.Unmarshal([]byte(cached), &flight); err == nil {
			return &pb.GetFlightResponse{Flight: &flight}, nil
		}
	} else {
		log.Printf("Cache MISS for key: %s", cacheKey)
	}

	var f Flight
	err = s.db.QueryRowContext(ctx, `
		SELECT id, flight_number, airline, origin_airport, destination_airport,
		       departure_time, arrival_time, total_seats, available_seats, price,
		       status, created_at, updated_at
		FROM flights WHERE id = $1
	`, req.FlightId).Scan(&f.ID, &f.FlightNumber, &f.Airline, &f.OriginAirport, &f.DestinationAirport,
		&f.DepartureTime, &f.ArrivalTime, &f.TotalSeats, &f.AvailableSeats, &f.Price,
		&f.Status, &f.CreatedAt, &f.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "flight not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}

	flight := &pb.Flight{
		Id:                 f.ID,
		FlightNumber:       f.FlightNumber,
		Airline:            f.Airline,
		OriginAirport:      f.OriginAirport,
		DestinationAirport: f.DestinationAirport,
		DepartureTime:      timestamppb.New(f.DepartureTime),
		ArrivalTime:        timestamppb.New(f.ArrivalTime),
		TotalSeats:         f.TotalSeats,
		AvailableSeats:     f.AvailableSeats,
		Price:              f.Price,
		Status:             mapFlightStatus(f.Status),
		CreatedAt:          timestamppb.New(f.CreatedAt),
		UpdatedAt:          timestamppb.New(f.UpdatedAt),
	}

	data, _ := json.Marshal(flight)
	s.redis.Set(ctx, cacheKey, data, s.cacheTTL)

	return &pb.GetFlightResponse{Flight: flight}, nil
}

func (s *server) ReserveSeats(ctx context.Context, req *pb.ReserveSeatRequest) (*pb.ReserveSeatResponse, error) {
	log.Printf("ReserveSeats: flight_id=%d, seat_count=%d, booking_id=%s", req.FlightId, req.SeatCount, req.BookingId)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, status.Error(codes.Internal, "transaction error")
	}
	defer tx.Rollback()

	var existingID int64
	err = tx.QueryRowContext(ctx, "SELECT id FROM seat_reservations WHERE booking_id = $1", req.BookingId).Scan(&existingID)
	if err == nil {
		log.Printf("Idempotent request: reservation already exists for booking_id=%s", req.BookingId)
		tx.Commit()
		return &pb.ReserveSeatResponse{
			ReservationId: existingID,
			Success:       true,
			Message:       "reservation already exists",
		}, nil
	}

	var availableSeats int32
	err = tx.QueryRowContext(ctx, "SELECT available_seats FROM flights WHERE id = $1 FOR UPDATE", req.FlightId).Scan(&availableSeats)
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "flight not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}

	if availableSeats < req.SeatCount {
		return nil, status.Error(codes.ResourceExhausted, "not enough seats available")
	}

	_, err = tx.ExecContext(ctx, "UPDATE flights SET available_seats = available_seats - $1, updated_at = NOW() WHERE id = $2", req.SeatCount, req.FlightId)
	if err != nil {
		return nil, status.Error(codes.Internal, "update error")
	}

	var reservationID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO seat_reservations (flight_id, booking_id, seat_count, status)
		VALUES ($1, $2, $3, 'ACTIVE')
		RETURNING id
	`, req.FlightId, req.BookingId, req.SeatCount).Scan(&reservationID)
	if err != nil {
		return nil, status.Error(codes.Internal, "reservation creation error")
	}

	if err := tx.Commit(); err != nil {
		return nil, status.Error(codes.Internal, "commit error")
	}

	s.invalidateCache(ctx, req.FlightId)

	return &pb.ReserveSeatResponse{
		ReservationId: reservationID,
		Success:       true,
		Message:       "seats reserved successfully",
	}, nil
}

func (s *server) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
	log.Printf("ReleaseReservation: booking_id=%s", req.BookingId)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, status.Error(codes.Internal, "transaction error")
	}
	defer tx.Rollback()

	var flightID int64
	var seatCount int32
	err = tx.QueryRowContext(ctx, `
		SELECT flight_id, seat_count FROM seat_reservations
		WHERE booking_id = $1 AND status = 'ACTIVE'
		FOR UPDATE
	`, req.BookingId).Scan(&flightID, &seatCount)

	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "active reservation not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}

	_, err = tx.ExecContext(ctx, "UPDATE flights SET available_seats = available_seats + $1, updated_at = NOW() WHERE id = $2", seatCount, flightID)
	if err != nil {
		return nil, status.Error(codes.Internal, "update error")
	}

	_, err = tx.ExecContext(ctx, "UPDATE seat_reservations SET status = 'RELEASED', updated_at = NOW() WHERE booking_id = $1", req.BookingId)
	if err != nil {
		return nil, status.Error(codes.Internal, "reservation update error")
	}

	if err := tx.Commit(); err != nil {
		return nil, status.Error(codes.Internal, "commit error")
	}

	s.invalidateCache(ctx, flightID)

	return &pb.ReleaseReservationResponse{
		Success: true,
		Message: "reservation released successfully",
	}, nil
}

func (s *server) invalidateCache(ctx context.Context, flightID int64) {
	cacheKey := fmt.Sprintf("flight:%d", flightID)
	s.redis.Del(ctx, cacheKey)
	log.Printf("Cache invalidated for key: %s", cacheKey)

	pattern := "search:*"
	iter := s.redis.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		s.redis.Del(ctx, iter.Val())
	}
	log.Printf("Cache invalidated for pattern: %s", pattern)
}

func mapFlightStatus(status string) pb.FlightStatus {
	switch strings.ToUpper(status) {
	case "SCHEDULED":
		return pb.FlightStatus_FLIGHT_STATUS_SCHEDULED
	case "DEPARTED":
		return pb.FlightStatus_FLIGHT_STATUS_DEPARTED
	case "CANCELLED":
		return pb.FlightStatus_FLIGHT_STATUS_CANCELLED
	case "COMPLETED":
		return pb.FlightStatus_FLIGHT_STATUS_COMPLETED
	default:
		return pb.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
	}
}

func runMigrations(db *sql.DB) error {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance("file://migrations", "postgres", driver)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}

	log.Println("Migrations applied successfully")
	return nil
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL not set")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := runMigrations(db); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	redisMaster := os.Getenv("REDIS_MASTER")
	if redisMaster == "" {
		redisMaster = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisMaster,
	})

	cacheTTL := 300 * time.Second
	if ttlStr := os.Getenv("CACHE_TTL"); ttlStr != "" {
		if ttl, err := strconv.Atoi(ttlStr); err == nil {
			cacheTTL = time.Duration(ttl) * time.Second
		}
	}

	apiKey := os.Getenv("GRPC_API_KEY")
	if apiKey == "" {
		log.Fatal("GRPC_API_KEY not set")
	}

	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(authInterceptor(apiKey)),
	)

	pb.RegisterFlightServiceServer(s, &server{
		db:       db,
		redis:    rdb,
		cacheTTL: cacheTTL,
		apiKey:   apiKey,
	})

	log.Printf("Flight Service listening on port %s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
