mockgen:
	go install github.com/golang/mock/mockgen@v1.6.0
	mockgen -source=internal/repository/public.go -destination=internal/repository/public_mock.gen.go -package=repository
	mockgen -source=internal/rabbitmq/notifier/public.go -destination=internal/rabbitmq/notifier/public_mock.gen.go -package=notifier

lint:
	golangci-lint run

test:
	go test -cover ./... -count=1

pre-commit:
	go mod tidy
	make mockgen
	make lint
	make test
