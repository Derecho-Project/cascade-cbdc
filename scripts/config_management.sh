#!/usr/bin/env bash
# gen_configs_from_inventory.sh
#
# Generates per-node configs from a JSON inventory into out_dir/<node_name>/.
#
# Requires: jq
#
# Usage:
#   ./gen_configs_from_inventory.sh inventory.json templates_dir out_dir
#
# templates_dir must contain:
#   - derecho.cfg.tmp
#   - derecho_node.cfg.tmp
#   - wanagent.json
#
# Inventory supports BOTH primary + backup leaders.
# Each node selects which leader to use via node.leader_role ("primary" or "backup").

set -euo pipefail

inv="${1:-}"
tmpl_dir="${2:-}"
out_dir="${3:-}"

[[ -n "${inv}" && -n "${tmpl_dir}" && -n "${out_dir}" ]] || {
    echo "usage: $0 <inventory.json> <templates_dir> <out_dir>" >&2
    exit 1
}

command -v jq >/dev/null 2>&1 || {
    echo "jq is required" >&2
    exit 1
}

main_tmpl="${tmpl_dir}/derecho.cfg.tmp"
node_tmpl="${tmpl_dir}/derecho_node.cfg.tmp"
wan_tmpl="${tmpl_dir}/wanagent.json"

for f in "${inv}" "${main_tmpl}" "${node_tmpl}" "${wan_tmpl}"; do
    [[ -f "${f}" ]] || {
        echo "missing file: ${f}" >&2
        exit 1
    }
done

mkdir -p "${out_dir}"

patch_derecho_section_kv() {
    # args: infile key value
    local infile="$1" key="$2" value="$3"
    awk -v k="${key}" -v v="${value}" '
    BEGIN{in_d=0}
    /^\[DERECHO\][[:space:]]*$/ {in_d=1; print; next}
    /^\[[^]]+\][[:space:]]*$/  {in_d=0; print; next}
    in_d && $0 ~ "^[[:space:]]*"k"[[:space:]]*=" { sub(/=.*/, "= " v); print; next }
    { print }
  ' "${infile}"
}

patch_section_kv() {
    # args: infile section_name key value
    local infile="$1" section="$2" key="$3" value="$4"
    awk -v sec="${section}" -v k="${key}" -v v="${value}" '
    BEGIN{cur=""}
    /^\[[^]]+\][[:space:]]*$/ {cur=$0; print; next}
    cur=="["sec"]" && $0 ~ "^[[:space:]]*"k"[[:space:]]*=" { sub(/=.*/, "= " v); print; next }
    { print }
  ' "${infile}"
}

# ---- validate inventory ----
jq -e '
  .leaders.primary.contact_ip and .leaders.primary.contact_port and
  .leaders.backup.contact_ip  and .leaders.backup.contact_port  and
  (.sites|type=="array") and
  (.nodes|type=="array") and
  (.nodes|length>0) and
  (.sites[] | (.ips|length)==(.ports|length))
' "${inv}" >/dev/null

primary_ip="$(jq -r '.leaders.primary.contact_ip' "${inv}")"
primary_port="$(jq -r '.leaders.primary.contact_port' "${inv}")"
backup_ip="$(jq -r '.leaders.backup.contact_ip' "${inv}")"
backup_port="$(jq -r '.leaders.backup.contact_port' "${inv}")"

sites_json="$(jq '.sites | sort_by(.id)' "${inv}")"

node_count="$(jq '.nodes|length' "${inv}")"

for ((idx = 0; idx < node_count; idx++)); do
    node_name="$(jq -r ".nodes[$idx].name" "${inv}")"
    local_ip="$(jq -r ".nodes[$idx].local_ip" "${inv}")"
    vlan_if="$(jq -r ".nodes[$idx].vlan_if" "${inv}")"
    site_id="$(jq -r ".nodes[$idx].site_id" "${inv}")"
    leader_role="$(jq -r ".nodes[$idx].leader_role // \"primary\"" "${inv}")"

    [[ -n "${node_name}" && "${node_name}" != "null" ]] || {
        echo "node[$idx] missing name" >&2
        exit 1
    }
    [[ -n "${local_ip}" && "${local_ip}" != "null" ]] || {
        echo "node[$idx] missing local_ip" >&2
        exit 1
    }
    [[ -n "${vlan_if}" && "${vlan_if}" != "null" ]] || {
        echo "node[$idx] missing vlan_if" >&2
        exit 1
    }
    [[ -n "${site_id}" && "${site_id}" != "null" ]] || {
        echo "node[$idx] missing site_id" >&2
        exit 1
    }
    [[ "${leader_role}" == "primary" || "${leader_role}" == "backup" ]] || {
        echo "node[$idx] leader_role must be 'primary' or 'backup' (got '${leader_role}')" >&2
        exit 1
    }

    if [[ "${leader_role}" == "primary" ]]; then
        leader_ip="${primary_ip}"
        leader_port="${primary_port}"
    else
        leader_ip="${backup_ip}"
        leader_port="${backup_port}"
    fi

    node_out="${out_dir}/${node_name}"
    mkdir -p "${node_out}"

    # ---- derecho.cfg.tmp (leader contact info based on node's leader_role) ----
    tmp="$(mktemp)"
    patch_derecho_section_kv "${main_tmpl}" "contact_ip" "${leader_ip}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho.cfg.tmp"

    tmp="$(mktemp)"
    patch_derecho_section_kv "${node_out}/derecho.cfg.tmp" "contact_port" "${leader_port}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho.cfg.tmp"

    # ---- derecho_node.cfg.tmp (node local ip + [RDMA].domain) ----
    tmp="$(mktemp)"
    patch_derecho_section_kv "${node_tmpl}" "local_ip" "${local_ip}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho_node.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho_node.cfg.tmp" "RDMA" "domain" "${vlan_if}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho_node.cfg.tmp"

    # ---- wanagent.json (node private_ip + local_site_id + sites with ips/ports) ----
    tmp="$(mktemp)"
    jq \
        --arg ip "${local_ip}" \
        --argjson sid "${site_id}" \
        --argjson sites "${sites_json}" \
        '
      .private_ip = $ip
      | .local_site_id = $sid
      | .sites = $sites
    ' "${wan_tmpl}" >"${tmp}"
    mv "${tmp}" "${node_out}/wanagent.json"

    echo "generated: ${node_out}  (leader_role=${leader_role})"
done

echo "done. scp each folder under ${out_dir}/<node_name>/ to its node."
