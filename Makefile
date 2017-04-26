GO     ?= go
DEP    ?= dep

EPAXOSSRC := ./epaxos

.PHONY: test
test:
	@$(GO) test -v $(EPAXOSSRC)/...

.PHONY: dep
dep:
	@$(DEP) ensure -update

.PHONY: proto
proto:
	@$(MAKE) -C epaxos/epaxospb       regenerate
	@$(MAKE) -C transport/transportpb regenerate

.PHONY: check
check:
	@$(GO) vet $(EPAXOSSRC)/...
