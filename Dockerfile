FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json postcss.config.js tailwind.config.js /frontend/
RUN cd /frontend && npm ci

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app
RUN TAILWIND_PROJECT_ROOT=/app /frontend/node_modules/.bin/postcss /app/crm/static_src/crm/tailwind.css -o /app/crm/static/crm/tailwind.css

# Rebuild static assets on boot so a fresh checkout works with DEBUG=False.
CMD ["bash", "-lc", "python -m django --version && python manage.py collectstatic --noinput && if [ \"$DEBUG\" = \"True\" ]; then exec gunicorn config.wsgi:application --reload --bind 0.0.0.0:8000; else exec gunicorn config.wsgi:application --bind 0.0.0.0:8000; fi"]
