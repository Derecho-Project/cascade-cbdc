#!/usr/bin/env bash
# update_derecho_conf.sh
#
# Usage:
#   ./update_derecho_conf.sh derecho.cfg.tmp
#
# Updates:
#   - derecho.cfg.tmp        : contact_ip (prompt), contact_port = 23581
#   - derecho_node.cfg.tmp   : local_ip (auto from `ip a`)
#   - wanagent.json          :
#       * private_ip = local_ip
#       * sites[0].ips = user-entered IP list (count chosen by user)
#       * sites[1].ips = user-entered IP list (count chosen by user)
#
# Requires: jq

set -euo pipefail

main_cfg="${1:-}"
node_cfg="derecho_node.cfg.tmp"
wan_cfg="wanagent.json"
CONTACT_PORT=23581

if [[ -z "${main_cfg}" ]]; then
    echo "usage: $0 <derecho.cfg.tmp>" >&2
    exit 1
fi

for f in "${node_cfg}" "${wan_cfg}"; do
    [[ -f "${f}" ]] || {
        echo "missing ${f} in current directory" >&2
        exit 1
    }
done

command -v jq >/dev/null 2>&1 || {
    echo "jq is required" >&2
    exit 1
}

read -rp "Enter contact_ip (leader IP): " contact_ip
[[ -n "${contact_ip}" ]] || {
    echo "contact_ip cannot be empty" >&2
    exit 1
}

read -rp "How many IPs for site 0? " site0_n
read -rp "How many IPs for site 1? " site1_n
[[ "${site0_n}" =~ ^[0-9]+$ ]] || {
    echo "site 0 count must be an integer" >&2
    exit 1
}
[[ "${site1_n}" =~ ^[0-9]+$ ]] || {
    echo "site 1 count must be an integer" >&2
    exit 1
}

# Collect IPs (space-separated OK, one per prompt)
site0_ips=()
for ((i = 0; i < site0_n; i++)); do
    read -rp "site 0 ip[$i]: " ip
    [[ -n "${ip}" ]] || {
        echo "empty ip not allowed" >&2
        exit 1
    }
    site0_ips+=("${ip}")
done

site1_ips=()
for ((i = 0; i < site1_n; i++)); do
    read -rp "site 1 ip[$i]: " ip
    [[ -n "${ip}" ]] || {
        echo "empty ip not allowed" >&2
        exit 1
    }
    site1_ips+=("${ip}")
done

# First non-loopback IPv4 for local_ip + private_ip
local_ip="$(
    ip -4 -o addr show scope global |
        awk '{print $4}' |
        cut -d/ -f1 |
        head -n1
)"
[[ -n "${local_ip}" ]] || {
    echo "Could not find a global (non-loopback) IPv4 from 'ip a'" >&2
    exit 1
}

update_main_cfg() {
    awk -v c_ip="${contact_ip}" -v c_port="${CONTACT_PORT}" '
    BEGIN { in_derecho=0 }
    /^\[DERECHO\][[:space:]]*$/ { in_derecho=1; print; next }
    /^\[[^]]+\][[:space:]]*$/  { in_derecho=0; print; next }

    in_derecho && /^[[:space:]]*contact_ip[[:space:]]*=/ {
      sub(/=.*/, "= " c_ip); print; next
    }
    in_derecho && /^[[:space:]]*contact_port[[:space:]]*=/ {
      sub(/=.*/, "= " c_port); print; next
    }
    { print }
  ' "$1"
}

update_node_cfg() {
    awk -v l_ip="${local_ip}" '
    BEGIN { in_derecho=0 }
    /^\[DERECHO\][[:space:]]*$/ { in_derecho=1; print; next }
    /^\[[^]]+\][[:space:]]*$/  { in_derecho=0; print; next }

    in_derecho && /^[[:space:]]*local_ip[[:space:]]*=/ {
      sub(/=.*/, "= " l_ip); print; next
    }
    { print }
  ' "$1"
}

# ---- Apply updates ----

# derecho.cfg.tmp
tmp="$(mktemp)"
update_main_cfg "${main_cfg}" >"${tmp}"
mv "${tmp}" "${main_cfg}"

# derecho_node.cfg.tmp
tmp="$(mktemp)"
update_node_cfg "${node_cfg}" >"${tmp}"
mv "${tmp}" "${node_cfg}"

# Build JSON arrays for jq from bash arrays
site0_json="$(printf '%s\n' "${site0_ips[@]}" | jq -R . | jq -s .)"
site1_json="$(printf '%s\n' "${site1_ips[@]}" | jq -R . | jq -s .)"

# wanagent.json
tmp="$(mktemp)"
jq \
    --arg ip "${local_ip}" \
    --argjson s0 "${site0_json}" \
    --argjson s1 "${site1_json}" \
    '
  .private_ip = $ip
  | .sites |= map(
      if .id == 0 then .ips = $s0
      elif .id == 1 then .ips = $s1
      else .
      end
    )
  ' "${wan_cfg}" >"${tmp}"
mv "${tmp}" "${wan_cfg}"

echo "Updated:"
echo "  ${main_cfg}        -> contact_ip=${contact_ip}, contact_port=${CONTACT_PORT}"
echo "  ${node_cfg}   -> local_ip=${local_ip}"
echo "  ${wan_cfg}          -> private_ip=${local_ip}"
echo "                       site 0 ips = ${site0_n} entries"
echo "                       site 1 ips = ${site1_n} entries"
