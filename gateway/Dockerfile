FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /bin/service ./gateway

FROM scratch
COPY --from=build /bin/service /service
EXPOSE 8080
ENTRYPOINT ["/service"]
