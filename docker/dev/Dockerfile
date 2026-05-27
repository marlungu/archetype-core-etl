FROM python:3.12-slim-bookworm

# Create non-root user for sandboxing
RUN groupadd --gid 1000 dev && \
    useradd --uid 1000 --gid dev --create-home --shell /bin/bash dev

WORKDIR /workspace

# Install system deps needed for psycopg2 and similar
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
      curl \
      git \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps as root, then drop to non-root.
# Only pyproject.toml is copied; the real source lands via volume mount at
# runtime.  We create stubs so hatchling can validate project metadata and
# write the editable .pth file — the stub files are never imported.
#
# We install [dev] + apache-airflow directly rather than [dev,airflow] to avoid
# pulling in apache-airflow-providers-fab, which depends on flask-appbuilder
# 4.6.x → SQLAlchemy<1.5, conflicting with apache-airflow-core ≥3.2 that needs
# SQLAlchemy≥2.0.  The FAB provider is only required inside the Airflow runtime
# containers (it is installed there via _PIP_ADDITIONAL_REQUIREMENTS), not in
# the dev container that runs tests and type-checking.
COPY pyproject.toml /workspace/
RUN touch /workspace/README.md && \
    mkdir -p /workspace/src/archetype_core_etl && \
    touch /workspace/src/archetype_core_etl/__init__.py && \
    pip install --no-cache-dir -e ".[dev]" "apache-airflow~=3.2" && \
    rm /workspace/README.md /workspace/src/archetype_core_etl/__init__.py && \
    # Pre-create the cache dir with dev ownership so the bind-mount in
    # docker-compose.yml inherits the right permissions on first use.
    mkdir -p /workspace/.cache && \
    chown -R dev:dev /workspace/.cache

USER dev

CMD ["bash"]
