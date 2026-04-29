FROM grafana/grafana:latest
USER root

# Install dependencies
RUN apk add --no-cache unzip curl bash

# Download and install Loki and Alloy
RUN cd /usr/local/bin && \
    curl -O -L "https://github.com/grafana/loki/releases/download/v3.5.3/loki-linux-amd64.zip" && \
    curl -O -L "https://github.com/grafana/alloy/releases/download/v1.11.3/alloy-linux-amd64.zip" && \
    unzip loki-linux-amd64.zip && \
    unzip "alloy-linux-amd64.zip" && \
    rm loki-linux-amd64.zip alloy-linux-amd64.zip && \
    chmod +x loki-linux-amd64 alloy-linux-amd64

# Create system users
RUN addgroup -S loki && adduser -S loki -G loki && \
    addgroup -S alloy && adduser -S alloy -G alloy

# Copy config files
COPY loki-local-config.yaml /etc/loki/local-config.yaml
COPY config.alloy /etc/alloy/config.alloy
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create required directories
RUN mkdir -p /etc/loki /etc/alloy /loki-data

# Expose ports
EXPOSE 3000 3100 12345

ENTRYPOINT ["/entrypoint.sh"]