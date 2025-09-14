# ---- build stage ----
FROM golang:1.24.3 AS build
WORKDIR /app

# Speed up subsequent builds by caching modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY . .

# Build the binary at /app/server (inside the image)
# If you want maximum compatibility with App Runner, force amd64:
# ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -o server .

# ---- run stage (small, secure base) ----
FROM gcr.io/distroless/base-debian12
WORKDIR /app

# Copy the compiled binary from the build stage
COPY --from=build /app/server /app/server

# Your app listens on 5050
EXPOSE 5050
ENV PORT=5050

USER nonroot:nonroot
ENTRYPOINT ["/app/server"]