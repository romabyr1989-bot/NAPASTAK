FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DFO_REST_URL=http://dfo-gateway:8080
ENV DFO_TOKEN=""
ENV SOAP_PORT=3000
ENV REST_PORT=8443
ENV CDI_PORT=8444

EXPOSE 3000 8443 8444

CMD ["python3", "main.py"]
