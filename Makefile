PRE_COMMIT = pre-commit
PRE_COMMIT_RUN_ARGS = --all-files
PRE_COMMIT_INSTALL_ARGS = --install-hooks

.PHONY: lint
lint:
	$(PRE_COMMIT) run $(PRE_COMMIT_RUN_ARGS)

.PHONY: pre-commit-install
pre-commit-install:
	$(PRE_COMMIT) install $(PRE_COMMIT_INSTALL_ARGS)