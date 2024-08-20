FROM golang:1.22-alpine

WORKDIR /app

# Install git and protobuf
RUN apk add --no-cache git protobuf

# Install protoc-gen-go and protoc-gen-go-grpc
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           $(find . -name "*.proto")

RUN go build -o main

EXPOSE 50051

CMD ["./main"]