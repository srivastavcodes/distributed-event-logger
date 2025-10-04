FROM golang:1.24-alpine AS build

WORKDIR /cubelog

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /cubelog/bin/cubelog ./cmd/cubelog

FROM scratch

COPY --from=build /cubelog/bin/cubelog /bin/cubelog

ENTRYPOINT ["/bin/cubelog"]
