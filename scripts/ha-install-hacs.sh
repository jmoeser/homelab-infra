#!/usr/bin/env bash
# ha-install-hacs.sh — Install HACS into the Home Assistant config volume
#
# Run this ONCE on maelthra after the homeassistant container first starts.
# After HACS is installed, restart HA, then configure HACS in the UI:
#   Settings → Devices & Services → Add Integration → search "HACS"
#
# Usage (run as root on maelthra):
#   ./scripts/ha-install-hacs.sh

set -euo pipefail

CONTAINER=homeassistant
VOLUME=homeassistant-config

if ! podman container exists "$CONTAINER" 2>/dev/null; then
    echo "Error: container '$CONTAINER' not found. Is Home Assistant running?" >&2
    exit 1
fi

if podman exec "$CONTAINER" test -d /config/custom_components/hacs 2>/dev/null; then
    echo "HACS is already installed at /config/custom_components/hacs — nothing to do."
    exit 0
fi

echo "Stopping Home Assistant..."
podman stop "$CONTAINER"

echo "Downloading and installing HACS into volume '$VOLUME'..."
podman run --rm \
    --volume "${VOLUME}.volume:/config:Z" \
    docker.io/library/alpine:3 \
    sh -c '
        set -e
        apk add --quiet --no-progress wget unzip ca-certificates
        HACS_VERSION=$(wget -qO- https://api.github.com/repos/hacs/integration/releases/latest \
            | grep -o "\"tag_name\": \"[^\"]*\"" | cut -d\" -f4)
        echo "Installing HACS ${HACS_VERSION}..."
        mkdir -p /config/custom_components
        wget -qO /tmp/hacs.zip \
            "https://github.com/hacs/integration/releases/download/${HACS_VERSION}/hacs.zip"
        unzip -q /tmp/hacs.zip -d /config/custom_components/hacs
        echo "Done. HACS installed to /config/custom_components/hacs"
    '

echo "Restarting Home Assistant..."
systemctl start homeassistant.service

echo ""
echo "HACS installed. Next steps:"
echo "  1. Wait for Home Assistant to finish starting (~60s)"
echo "  2. Open Home Assistant and go to:"
echo "     Settings → Devices & Services → Add Integration → search 'HACS'"
echo "  3. Follow the HACS onboarding (GitHub auth required)"
echo "  4. In HACS → Integrations → search 'Meross LAN' → Download"
echo "  5. Restart HA, then add Meross LAN via Settings → Integrations"
