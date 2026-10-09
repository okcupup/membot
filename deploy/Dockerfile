# The exact base is overridden only by the reviewed image lock in Compose.
ARG PYTHON_IMAGE=python:3.11.11-slim-bookworm
FROM ${PYTHON_IMAGE} AS build
ARG PIP_NO_INDEX=0
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 membot_SKIP_WEBUI_BUILD=1
WORKDIR /build
COPY deploy/build-requirements.lock /build/build-requirements.lock
RUN --mount=type=bind,source=deploy/wheelhouse,target=/wheelhouse \
    if [ "$PIP_NO_INDEX" = "1" ]; then EXTRA=--no-index; else EXTRA=; fi; \
    python -m pip install --no-cache-dir $EXTRA --find-links=/wheelhouse -r build-requirements.lock
COPY . /build/source
RUN python -m pip wheel --no-deps --no-build-isolation --wheel-dir /wheel /build/source

FROM ${PYTHON_IMAGE} AS runtime
ARG PIP_NO_INDEX=0
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LITELLM_LOCAL_MODEL_COST_MAP=True HOME=/var/lib/membot MEMBOT_WORKSPACE=/var/lib/membot/workspace
COPY deploy/requirements.lock /tmp/requirements.lock
RUN --mount=type=bind,source=deploy/wheelhouse,target=/wheelhouse \
    if [ "$PIP_NO_INDEX" = "1" ]; then EXTRA=--no-index; else EXTRA=; fi; \
    python -m pip install --no-cache-dir $EXTRA --find-links=/wheelhouse -r /tmp/requirements.lock
COPY --from=build /wheel /tmp/wheel
RUN python -m pip install --no-cache-dir --no-deps /tmp/wheel/*.whl && python -m pip check \
    && groupadd --gid 10001 membot && useradd --uid 10001 --gid 10001 --no-create-home membot \
    && mkdir -p /var/lib/membot/workspace && chown -R 10001:10001 /var/lib/membot
USER 10001:10001
WORKDIR /var/lib/membot/workspace
RUN python -c "import membot.agent.loop, membot.cli.commands, membot.service.entrypoints; from importlib.resources import files; assert (files('membot.agent.persistence')/'migrations'/'0003_worker_health.sql').is_file(); assert (files('membot')/'templates'/'AGENTS.md').is_file()"
ENTRYPOINT ["membot-service"]
CMD ["api"]
