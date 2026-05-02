#!/bin/bash
# Opens the P2P port (4001) in the system firewall (ufw or iptables)

PORT=4001

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root: sudo $0"
    exit 1
fi

if command -v ufw &>/dev/null && ufw status | grep -q "Status: active"; then
    echo "Detected ufw..."
    ufw allow "$PORT/tcp"
    ufw allow "$PORT/udp"
    echo "Port $PORT opened via ufw."

elif command -v iptables &>/dev/null; then
    echo "Detected iptables..."
    iptables -C INPUT -p tcp --dport "$PORT" -j ACCEPT 2>/dev/null || \
        iptables -I INPUT -p tcp --dport "$PORT" -j ACCEPT
    iptables -C INPUT -p udp --dport "$PORT" -j ACCEPT 2>/dev/null || \
        iptables -I INPUT -p udp --dport "$PORT" -j ACCEPT

    if command -v iptables-save &>/dev/null; then
        iptables-save > /etc/iptables/rules.v4 2>/dev/null || true
    fi
    echo "Port $PORT opened via iptables."

else
    echo "No supported firewall found (ufw or iptables)."
    exit 1
fi

echo "Done. Verify with: ss -tlnup | grep $PORT"