# Multi-stage build: compile in a full Go image, ship only the binary.
# Final image is ~15 MB, has no shell or package manager (smaller attack
# surface), and runs as a non-root user.

FROM golang:1.25 AS build
WORKDIR /src

# Copy go.mod/go.sum first so the dependency download is cached as its own
# layer and only re-runs when dependencies change, not on every code edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 -> fully static binary, so it runs on a distroless base with no libc.
# loadtest ships in the same image so Step 4 can generate load from inside the
# cluster (a pod), instead of through a slow `kubectl port-forward`.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadtest ./cmd/loadtest

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /out/loadtest /
EXPOSE 2112
USER nonroot:nonroot
ENTRYPOINT ["/worker"]
