#mockgen:
#	go install github.com/golang/mock/mockgen@v1.6.0
#	mockgen -source=internal/repository/public.go -destination=internal/repository/public_mock.gen.go -package=repository
#	mockgen -source=internal/kafka/writer/writer.go -destination=internal/kafka/writer/riter.gen.go -package=kafka

lint:
	golangci-lint run

test:
	go test -cover ./... -count=1

pre-commit:
	go mod tidy
	#make mockgen
	make lint
	make test
