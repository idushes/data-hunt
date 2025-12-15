# Use a multi-stage build to keep the final image size small

# Stage 1: Builder
# We use the official uv image to install dependencies
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# Set the working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
# --frozen: Sync with uv.lock
# --no-install-project: We only want dependencies, not the project itself yet
RUN uv sync --frozen --no-install-project

# Stage 2: Final
# We use a slim python image for the runtime
FROM python:3.13-slim

# Set the working directory
WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy the application code
# Copy the application code
COPY server.py config.py tasks.py database.py models.py alembic.ini ./
COPY alembic ./alembic
COPY routers ./routers

# Set environment variables to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PORT=8111
ENV PYTHONUNBUFFERED=1

# Expose the port the app runs on
EXPOSE 8111

# Command to run the application
CMD ["gunicorn", "server:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8111", "--access-logfile", "-", "--error-logfile", "-"]
