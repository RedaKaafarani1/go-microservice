# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/server/main.go

LABEL org.opencontainers.image.source=https://github.com/redakaafarani1/go-microservice
LABEL org.opencontainers.image.description="CSV processor"
LABEL org.opencontainers.image.licenses=MIT

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates

# Set working directory
WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/server .

# Create a directory for the CSV file
RUN mkdir -p /data

# Expose the port the server runs on
EXPOSE 8080

# Command to run the executable
CMD ["./server"]
