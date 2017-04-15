GO     ?= go
DEP    ?= dep

TARGETS := 
PKGS := $(shell go list ./... | grep -v /vendor)

.PHONY: build
build:
	@for target in $(TARGETS) ; do \
		$(GO) build ./cmd/$$target ; \
	done

.PHONY: clean
clean:
	@$(RM) $(TARGETS)

.PHONY: test
test:
	@$(GO) test -v $(PKGS)

.PHONY: dep
dep:
	@$(DEP) ensure -update

.PHONY: proto
proto:
	@$(MAKE) -C epaxos/epaxospb regenerate

.PHONY: check
check:
	@$(GO) vet $(PKGS)
