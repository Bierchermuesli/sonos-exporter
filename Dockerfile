FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sonos_exporter.py .

# ICMP works without root when the host/container allows unprivileged
# ping sockets (net.ipv4.ping_group_range); see docker-compose.yaml.
RUN useradd --system --no-create-home sonos
USER sonos

EXPOSE 9814

ENTRYPOINT ["python", "sonos_exporter.py"]
