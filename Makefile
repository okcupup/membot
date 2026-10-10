PYTHON ?= ./.venv/bin/python
ENV_FILE ?= .env
DEPLOY = $(PYTHON) scripts/deploy.py --env-file $(ENV_FILE)
.PHONY: deploy-init-local deploy-config deploy-up deploy-down deploy-health deploy-doctor deploy-smoke deploy-api-drill deploy-worker-drill deploy-restart-check deploy-backup deploy-restore-check deploy-rate-check wheel-check test regression eval-queue eval-real eval-compare
deploy-init-local:
	$(DEPLOY) init-local
deploy-config:
	$(DEPLOY) config
deploy-up:
	$(DEPLOY) up
deploy-down:
	$(DEPLOY) down
deploy-health:
	$(DEPLOY) health
deploy-doctor:
	$(DEPLOY) doctor
deploy-smoke:
	$(DEPLOY) smoke
deploy-api-drill:
	$(DEPLOY) api-drill
deploy-worker-drill:
	$(DEPLOY) worker-drill
deploy-restart-check:
	$(DEPLOY) restart-check
deploy-backup:
	$(DEPLOY) backup
deploy-restore-check:
	$(DEPLOY) restore-check --backup $(BACKUP)
deploy-rate-check:
	$(DEPLOY) rate-check
wheel-check:
	$(PYTHON) scripts/wheel_check.py
test:
	LITELLM_LOCAL_MODEL_COST_MAP=True $(PYTHON) -m pytest -q -rs
regression:
	LITELLM_LOCAL_MODEL_COST_MAP=True $(PYTHON) scripts/evaluate_cases.py --mode deterministic --assert-count 32
	LITELLM_LOCAL_MODEL_COST_MAP=True $(PYTHON) -m pytest -q tests/regression
eval-queue:
	$(PYTHON) scripts/evaluate_cases.py --mode queue --assert-count 12
eval-real:
	$(PYTHON) scripts/evaluate_cases.py --mode real --real-config $(REAL_CONFIG) --repeat $(or $(REPEAT),3)
eval-compare:
	$(PYTHON) scripts/evaluate_cases.py --mode baseline --baseline $(BASELINE) --report $(REPORT) --assert-report
