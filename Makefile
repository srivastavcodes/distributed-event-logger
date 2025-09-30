CONFIG_PATH=./certs/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca security/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=security/ca-config.json \
		-profile=server \
		security/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=security/ca-config.json \
		-profile=client \
		security/client-csr.json | cfssljson -bare client

	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	protoc protolog/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

.PHONY: test
test:
	go test -race -v ./internal/$(PKG)
