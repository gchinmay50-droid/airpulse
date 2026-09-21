# One image for the three Python services (producer, sink, api).
# Which one runs is chosen by the compose `command`.
FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY api/ api/
# The gold publisher applies these to the cloud DB.
COPY sql/ sql/

# Non-root: nothing here needs privileges.
RUN useradd -r -u 1001 airpulse
USER airpulse

# Shell form so $PORT (set by Render/Cloud Run) is honoured; 8010 locally.
CMD python -m uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8010}
