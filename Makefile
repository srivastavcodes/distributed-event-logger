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
		-cn="root" \
		security/client-csr.json | cfssljson -bare root client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=security/ca-config.json \
		-profile=client \
		-cn="nobody" \
		security/client-csr.json | cfssljson -bare nobody client

	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	protoc protolog/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

$(CONFIG_PATH)/model.conf:
	cp security/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp security/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race -v ./internal/$(PKG)
