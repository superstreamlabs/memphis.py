PYTHON = python
PRE_COMMIT = $(PYTHON) -m pre_commit
PRE_COMMIT_RUN_ARGS = --all-files
PRE_COMMIT_INSTALL_ARGS = --install-hooks

HATCH = $(PYTHON) -m hatch
HATCH_VERSION =
.PHONY: lint
lint:
	$(PRE_COMMIT) run $(PRE_COMMIT_RUN_ARGS)

.PHONY: pre-commit-install
pre-commit-install:
	$(PRE_COMMIT) install $(PRE_COMMIT_INSTALL_ARGS)

.PHONY: version
version:
	@$(HATCH) version

.PHONY: bump_version
bump_version:
	$(HATCH) version $(HATCH_VERSION)