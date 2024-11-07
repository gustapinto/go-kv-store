.PHONY: gen

gen:
	protoc --proto_path=./proto --go_out=gen --go_opt=paths=source_relative record.proto
