# Use a small Python image
FROM python:3.12-slim

# Create app directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy exporter script
COPY sonos_exporter.py .

# Run as non-root user for safety
# RUN useradd -m sonos
# USER sonos

# Expose Prometheus exporter port
EXPOSE 9814

# Command line args can override default Sonos IP
ENTRYPOINT ["python", "sonos_exporter.py"]
CMD ["--sonos-ip", "100.88.88.41", "--log-level", "DEBUG"]
