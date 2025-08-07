FROM python:3.13.5-bookworm AS builder
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /opt/app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

FROM python:3.13.5-slim-bookworm
RUN apt-get update && \
    apt-get install -y --no-install-recommends libgomp1 && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
RUN useradd --create-home --shell /bin/bash appuser
COPY --chown=appuser:appuser src ./src
COPY --chown=appuser:appuser outputs ./outputs
USER appuser
ENV PATH="/opt/venv/bin:$PATH"
ENV MODEL_PATH="/app/outputs/models/lgbm_final_model.joblib"
ENTRYPOINT ["python", "src/consumer.py"]
