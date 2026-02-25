#!/usr/bin/env bash
# scripts/pg-backup.sh — Automated PostgreSQL backup to Backblaze B2
#
# Workflow:
#   1. pg_dump from the Postgres container
#   2. Compress with gzip
#   3. Encrypt with age (same key used for SOPS secrets)
#   4. Upload to Backblaze B2 via rclone
#   5. Prune backups older than retention period
#
# Deployed to: /var/lib/homelab-gitops/scripts/pg-backup.sh
# Triggered by: pg-backup.timer (systemd)

set -euo pipefail

# ---------------------------------------------------------------------------
# Config — overridable via environment
# ---------------------------------------------------------------------------
BACKUP_DIR="${PG_BACKUP_DIR:-/var/lib/homelab-gitops/backups}"
RCLONE_REMOTE="${PG_BACKUP_RCLONE_REMOTE:-b2}"
RCLONE_BUCKET="${PG_BACKUP_RCLONE_BUCKET:-homelab-pg-backups}"
RCLONE_CONFIG="${RCLONE_CONFIG:-/etc/homelab-gitops/rclone.conf}"
AGE_RECIPIENT_FILE="${PG_BACKUP_AGE_RECIPIENTS:-/etc/homelab-gitops/age-recipients.txt}"
AGE_KEY_FILE="${PG_BACKUP_AGE_KEY:-/etc/homelab-gitops/age-key.txt}"
RETENTION_DAYS="${PG_BACKUP_RETENTION_DAYS:-30}"
PG_CONTAINER="${PG_BACKUP_CONTAINER:-postgres}"
OPENCLAW_USER="${OPENCLAW_BACKUP_USER:-openclaw}"
OPENCLAW_WORKSPACE="${OPENCLAW_BACKUP_WORKSPACE:-/home/openclaw/workspace}"
LOG_ID="pg-backup"

# Read Postgres credentials from the deployed env file
PG_ENV_FILE="/etc/homelab-gitops/postgres.env"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "$*" | systemd-cat -t "${LOG_ID}" -p info;  echo "[INFO]  $*"; }
warn() { echo "$*" | systemd-cat -t "${LOG_ID}" -p warning; echo "[WARN]  $*"; }
err()  { echo "$*" | systemd-cat -t "${LOG_ID}" -p err;   echo "[ERROR] $*"; }
die()  { err "$*"; exit 1; }

cleanup() {
    rm -f "${BACKUP_DIR}/.tmp_"* 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------
preflight() {
    for cmd in podman rclone age gzip; do
        if ! command -v "${cmd}" &>/dev/null; then
            die "Required command not found: ${cmd}"
        fi
    done

    if [[ ! -f "${PG_ENV_FILE}" ]]; then
        die "Postgres env file not found: ${PG_ENV_FILE}"
    fi

    if [[ ! -f "${AGE_RECIPIENT_FILE}" ]]; then
        die "Age recipients file not found: ${AGE_RECIPIENT_FILE}"
    fi

    if [[ ! -f "${RCLONE_CONFIG}" ]]; then
        die "Rclone config not found: ${RCLONE_CONFIG}"
    fi

    # Ensure Postgres is running
    if ! podman exec "${PG_CONTAINER}" pg_isready -q 2>/dev/null; then
        die "PostgreSQL container '${PG_CONTAINER}' is not ready."
    fi

    mkdir -p "${BACKUP_DIR}"
}

# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------
do_backup() {
    local timestamp
    timestamp=$(date -u +"%Y%m%d-%H%M%S")

    # Read Postgres credentials
    # shellcheck source=/dev/null
    source "${PG_ENV_FILE}"
    local pg_user="${POSTGRES_USER:-postgres}"

    # Databases to back up
    local databases=("${FIREFLY_DB_NAME:-firefly}" "${GLOW_WORM_DB_NAME:-glow_worm}")

    for pg_db in "${databases[@]}"; do
        local basename="pg-backup-${pg_db}-${timestamp}"

        log "Starting backup: ${basename}"

        # Step 1: pg_dump from container → compressed SQL
        local dump_file="${BACKUP_DIR}/.tmp_${basename}.sql.gz"
        log "  Dumping database '${pg_db}' as user '${pg_user}'..."

        if ! podman exec "${PG_CONTAINER}" \
            pg_dump -U "${pg_user}" -d "${pg_db}" --format=plain --no-owner --no-acl \
            | gzip -9 > "${dump_file}"; then
            err "pg_dump failed for '${pg_db}'"
            continue
        fi

        local dump_size
        dump_size=$(du -h "${dump_file}" | cut -f1)
        log "  Dump complete: ${dump_size} compressed"

        # Step 2: Encrypt with age
        local encrypted_file="${BACKUP_DIR}/${basename}.sql.gz.age"
        log "  Encrypting with age..."

        if ! age --encrypt --recipients-file "${AGE_RECIPIENT_FILE}" \
            --output "${encrypted_file}" "${dump_file}"; then
            err "age encryption failed for '${pg_db}'"
            continue
        fi

        rm -f "${dump_file}"

        local enc_size
        enc_size=$(du -h "${encrypted_file}" | cut -f1)
        log "  Encrypted: ${enc_size}"

        # Step 3: Upload to B2
        log "  Uploading to ${RCLONE_REMOTE}:${RCLONE_BUCKET}..."

        if ! rclone --config "${RCLONE_CONFIG}" \
            copy "${encrypted_file}" "${RCLONE_REMOTE}:${RCLONE_BUCKET}/" \
            --progress --transfers 1; then
            warn "Upload failed for '${pg_db}'. Local backup retained at: ${encrypted_file}"
            continue
        fi

        log "  Upload complete."

        # Step 4: Remove local file (it's in B2 now)
        rm -f "${encrypted_file}"

        log "Backup '${basename}' completed successfully."
    done
}

# ---------------------------------------------------------------------------
# OpenClaw backup (config volume + workspace)
# ---------------------------------------------------------------------------
do_openclaw_backup() {
    local timestamp
    timestamp=$(date -u +"%Y%m%d-%H%M%S")

    # --- Config volume (contains memory, auth tokens, conversation history) ---
    local config_basename="openclaw-backup-config-${timestamp}"
    log "Starting backup: ${config_basename}"

    local config_dump="${BACKUP_DIR}/.tmp_${config_basename}.tar.gz"
    log "  Exporting openclaw-config volume as user '${OPENCLAW_USER}'..."

    local openclaw_uid
    openclaw_uid=$(id -u "${OPENCLAW_USER}")

    if ! runuser -u "${OPENCLAW_USER}" -- \
        env XDG_RUNTIME_DIR="/run/user/${openclaw_uid}" \
        podman volume export openclaw-config \
        | gzip -9 > "${config_dump}"; then
        err "Failed to export openclaw-config volume"
        return 1
    fi

    local config_size
    config_size=$(du -h "${config_dump}" | cut -f1)
    log "  Export complete: ${config_size} compressed"

    local config_enc="${BACKUP_DIR}/${config_basename}.tar.gz.age"
    log "  Encrypting with age..."

    if ! age --encrypt --recipients-file "${AGE_RECIPIENT_FILE}" \
        --output "${config_enc}" "${config_dump}"; then
        err "age encryption failed for openclaw-config"
        rm -f "${config_dump}"
        return 1
    fi
    rm -f "${config_dump}"

    log "  Uploading to ${RCLONE_REMOTE}:${RCLONE_BUCKET}..."
    if ! rclone --config "${RCLONE_CONFIG}" \
        copy "${config_enc}" "${RCLONE_REMOTE}:${RCLONE_BUCKET}/" \
        --progress --transfers 1; then
        warn "Upload failed for openclaw-config. Local backup retained: ${config_enc}"
        return 1
    fi
    rm -f "${config_enc}"
    log "Backup '${config_basename}' completed successfully."

    # --- Workspace directory ---
    if [[ -d "${OPENCLAW_WORKSPACE}" ]] && [[ -n "$(ls -A "${OPENCLAW_WORKSPACE}" 2>/dev/null)" ]]; then
        local ws_basename="openclaw-backup-workspace-${timestamp}"
        log "Starting backup: ${ws_basename}"

        local ws_dump="${BACKUP_DIR}/.tmp_${ws_basename}.tar.gz"
        log "  Archiving workspace ${OPENCLAW_WORKSPACE}..."

        if ! tar -czf "${ws_dump}" \
            -C "$(dirname "${OPENCLAW_WORKSPACE}")" \
            "$(basename "${OPENCLAW_WORKSPACE}")"; then
            err "Failed to archive openclaw workspace"
            rm -f "${ws_dump}"
            return 1
        fi

        local ws_size
        ws_size=$(du -h "${ws_dump}" | cut -f1)
        log "  Archive complete: ${ws_size} compressed"

        local ws_enc="${BACKUP_DIR}/${ws_basename}.tar.gz.age"
        if ! age --encrypt --recipients-file "${AGE_RECIPIENT_FILE}" \
            --output "${ws_enc}" "${ws_dump}"; then
            err "age encryption failed for openclaw workspace"
            rm -f "${ws_dump}"
            return 1
        fi
        rm -f "${ws_dump}"

        if ! rclone --config "${RCLONE_CONFIG}" \
            copy "${ws_enc}" "${RCLONE_REMOTE}:${RCLONE_BUCKET}/" \
            --progress --transfers 1; then
            warn "Upload failed for openclaw workspace. Local backup retained: ${ws_enc}"
            return 1
        fi
        rm -f "${ws_enc}"
        log "Backup '${ws_basename}' completed successfully."
    else
        log "Workspace ${OPENCLAW_WORKSPACE} is empty or missing — skipping."
    fi
}

# ---------------------------------------------------------------------------
# Home Assistant backup (config volume — includes DB, automations, integrations)
# ---------------------------------------------------------------------------
do_homeassistant_backup() {
    local timestamp
    timestamp=$(date -u +"%Y%m%d-%H%M%S")

    local basename="homeassistant-backup-${timestamp}"
    log "Starting backup: ${basename}"

    # Export live — HA uses SQLite WAL mode so a live export is safe enough
    # for a homelab. The critical config is in .storage/ (JSON, written atomically).
    local dump="${BACKUP_DIR}/.tmp_${basename}.tar.gz"
    log "  Exporting homeassistant-config volume..."

    if ! podman volume export homeassistant-config \
        | gzip -9 > "${dump}"; then
        err "Failed to export homeassistant-config volume"
        return 1
    fi

    local size
    size=$(du -h "${dump}" | cut -f1)
    log "  Export complete: ${size} compressed"

    local enc="${BACKUP_DIR}/${basename}.tar.gz.age"
    log "  Encrypting with age..."

    if ! age --encrypt --recipients-file "${AGE_RECIPIENT_FILE}" \
        --output "${enc}" "${dump}"; then
        err "age encryption failed for homeassistant-config"
        rm -f "${dump}"
        return 1
    fi
    rm -f "${dump}"

    log "  Uploading to ${RCLONE_REMOTE}:${RCLONE_BUCKET}..."
    if ! rclone --config "${RCLONE_CONFIG}" \
        copy "${enc}" "${RCLONE_REMOTE}:${RCLONE_BUCKET}/" \
        --progress --transfers 1; then
        warn "Upload failed for homeassistant-config. Local backup retained: ${enc}"
        return 1
    fi
    rm -f "${enc}"
    log "Backup '${basename}' completed successfully."
}

# ---------------------------------------------------------------------------
# Prune old backups
# ---------------------------------------------------------------------------
prune_remote() {
    log "Pruning backups older than ${RETENTION_DAYS} days..."

    # Calculate cutoff date
    local cutoff
    cutoff=$(date -u -d "${RETENTION_DAYS} days ago" +"%Y%m%d")

    # List remote files and delete old ones
    rclone --config "${RCLONE_CONFIG}" \
        lsf "${RCLONE_REMOTE}:${RCLONE_BUCKET}/" \
        --files-only 2>/dev/null | while IFS= read -r file; do

        # Extract date from any backup filename: *-YYYYMMDD-HHMMSS.*
        local file_date
        file_date=$(echo "${file}" | grep -oP '\d{8}(?=-\d{6}\.)' || echo "")

        if [[ -z "${file_date}" ]]; then
            continue
        fi

        if [[ "${file_date}" < "${cutoff}" ]]; then
            log "  Deleting old backup: ${file}"
            rclone --config "${RCLONE_CONFIG}" \
                deletefile "${RCLONE_REMOTE}:${RCLONE_BUCKET}/${file}" 2>/dev/null || \
                warn "  Failed to delete: ${file}"
        fi
    done

    # Also clean up any leftover local backups
    find "${BACKUP_DIR}" -name "pg-backup-*.sql.gz.age" -mtime "+${RETENTION_DAYS}" -delete 2>/dev/null || true
    find "${BACKUP_DIR}" -name "openclaw-backup-*.tar.gz.age" -mtime "+${RETENTION_DAYS}" -delete 2>/dev/null || true
    find "${BACKUP_DIR}" -name "homeassistant-backup-*.tar.gz.age" -mtime "+${RETENTION_DAYS}" -delete 2>/dev/null || true

    log "Pruning complete."
}

# ---------------------------------------------------------------------------
# Verify (optional — run manually to test a restore)
# ---------------------------------------------------------------------------
verify_latest() {
    log "Verifying latest backup..."

    local latest
    latest=$(rclone --config "${RCLONE_CONFIG}" \
        lsf "${RCLONE_REMOTE}:${RCLONE_BUCKET}/" \
        --files-only 2>/dev/null | sort | tail -1)

    if [[ -z "${latest}" ]]; then
        warn "No backups found in remote."
        return 1
    fi

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap "rm -rf '${tmp_dir}'" RETURN

    log "  Downloading: ${latest}"
    rclone --config "${RCLONE_CONFIG}" \
        copy "${RCLONE_REMOTE}:${RCLONE_BUCKET}/${latest}" "${tmp_dir}/"

    log "  Decrypting..."
    if age --decrypt --identity "${AGE_KEY_FILE}" \
        "${tmp_dir}/${latest}" | gzip -d | head -5 > /dev/null 2>&1; then
        log "  Verification passed — backup is valid."
    else
        err "  Verification FAILED — backup may be corrupted."
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    case "${1:-backup}" in
        backup)
            preflight
            do_backup
            do_openclaw_backup
            do_homeassistant_backup
            prune_remote
            ;;
        prune)
            prune_remote
            ;;
        verify)
            verify_latest
            ;;
        *)
            echo "Usage: $0 {backup|prune|verify}"
            exit 1
            ;;
    esac
}

main "$@"
