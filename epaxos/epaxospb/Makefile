PROTOC ?= protoc

VENDOR_DIR := ../../vendor
GOGO_PROTOBUF_PACKAGE := github.com/gogo/protobuf
GOGO_PROTOBUF_PATH := $(VENDOR_DIR)/$(GOGO_PROTOBUF_PACKAGE)
PROTOBUF_PATH  := $(GOGO_PROTOBUF_PATH)/protobuf

.PHONY: regenerate
regenerate:
	@$(PROTOC) --gogo_out=grpc:. -I=.:$(VENDOR_DIR):$(PROTOBUF_PATH) *.proto

.PHONY: clean
clean:
	@$(RM) *.pb.*
