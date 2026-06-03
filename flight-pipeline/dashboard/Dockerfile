# dashboard/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project (so imports work)
COPY . .

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Run from /app directory, pointing to dashboard/app.py
# This makes "from utils.data_loader import..." work correctly
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]