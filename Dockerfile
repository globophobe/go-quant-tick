FROM golang:1.26-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY quanttick ./quanttick

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/quanttick ./cmd/quanttick

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/quanttick /quanttick

USER nonroot:nonroot
ENTRYPOINT ["/quanttick"]
