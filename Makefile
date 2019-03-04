



all: hivefs

hivefs: *.go
	go build -o hivefs

clean:
	rm -f hivefs