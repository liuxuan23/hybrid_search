#!/usr/bin/env bash

set -euo pipefail

POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgresql}"
ARANGODB_SERVICE="${ARANGODB_SERVICE:-arangodb3}"
MODE="${1:-all}"

restart_service() {
    local service_name="$1"
    if command -v systemctl >/dev/null 2>&1; then
        echo "Restarting service: $service_name"
        sudo systemctl restart "$service_name"
        return
    fi
    if command -v service >/dev/null 2>&1; then
        echo "Restarting service: $service_name"
        sudo service "$service_name" restart
        return
    fi
    echo "Unable to restart $service_name automatically: no supported service manager found"
    return 1
}

print_drop_caches_hint() {
    cat <<'EOF'
Optional OS page cache clearing (Linux, requires root):
  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches
EOF
}

case "$MODE" in
    postgres)
        restart_service "$POSTGRES_SERVICE"
        ;;
    arangodb)
        restart_service "$ARANGODB_SERVICE"
        ;;
    all)
        restart_service "$POSTGRES_SERVICE"
        restart_service "$ARANGODB_SERVICE"
        ;;
    hint)
        ;;
    *)
        echo "Usage: $0 [postgres|arangodb|all|hint]" >&2
        exit 1
        ;;
esac

echo
print_drop_caches_hint
