GO     ?= go
DEP    ?= dep

TARGETS := server client

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
	@$(GO) test -v ./epaxos/...

.PHONY: dep
dep:
	@$(DEP) ensure -update

.PHONY: proto
proto:
	@$(MAKE) -C epaxos/epaxospb       regenerate
	@$(MAKE) -C transport/transportpb regenerate

.PHONY: check
check:
	@$(GO) vet $(PKGS)
