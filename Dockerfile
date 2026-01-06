FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/kvnode ./cmd/kvnode

FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=build /out/kvnode /kvnode
ENTRYPOINT ["/kvnode"]
