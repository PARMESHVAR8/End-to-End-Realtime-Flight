FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements-render.txt

EXPOSE 8501

CMD ["streamlit","run","dashboard/app.py","--server.address=0.0.0.0","--server.port=8501"]