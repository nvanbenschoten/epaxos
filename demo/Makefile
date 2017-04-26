GO ?= go

TARGETS := server client
TARGETDIR := bin

.PHONY: build
build:
	@for target in $(TARGETS) ; do \
		$(GO) build -o "$(TARGETDIR)/$$target" ./$$target ; \
	done

.PHONY: clean
clean:
	@$(RM) -r $(TARGETDIR)
