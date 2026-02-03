#!/usr/bin/env bash
# gen_configs_from_inventory.sh
#
# Usage:
#   ./gen_configs_from_inventory.sh inventory.json templates_dir out_dir
#
# templates_dir must contain:
#   - derecho.cfg.tmp
#   - derecho_node.cfg.tmp
#   - wanagent.json
#
# Requires: jq

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

patch_section_kv() {
    local infile="$1" section="$2" key="$3" value="$4"
    awk -v sec="${section}" -v k="${key}" -v v="${value}" '
    BEGIN{cur=""}
    /^\[[^]]+\][[:space:]]*$/ {cur=$0; print; next}
    cur=="["sec"]" && $0 ~ "^[[:space:]]*"k"[[:space:]]*=" { sub(/=.*/, "= " v); print; next }
    { print }
  ' "${infile}"
}

# Validate minimal shape
jq -e '
  .leaders.primary.contact_ip and .leaders.primary.contact_port and
  .leaders.backup.contact_ip  and .leaders.backup.contact_port  and
  (.nodes|type=="array") and (.nodes|length>0) and
  (.sites|type=="array") and (.sites|length>0) and
  (all(.nodes[]; has("name") and has("site_id") and has("leader_role")
              and (has("private_ip") or has("local_ip")) and has("vlan_if")
              and has("public_ip") and has("wanagent_port")))
' "${inv}" >/dev/null

primary_ip="$(jq -r '.leaders.primary.contact_ip' "${inv}")"
primary_port="$(jq -r '.leaders.primary.contact_port' "${inv}")"
backup_ip="$(jq -r '.leaders.backup.contact_ip' "${inv}")"
backup_port="$(jq -r '.leaders.backup.contact_port' "${inv}")"

# Precompute site ids (sorted) as compact JSON (array of numbers)
site_ids_json="$(jq -c '.sites | map(.id) | sort' "${inv}")"

derive_local_id() {
    local name="$1"
    if [[ "${name}" == "n4" ]]; then
        echo "101"
        return 0
    fi
    if [[ "${name}" =~ ^n([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]}"
        return 0
    fi
    echo "could not derive local_id from node name '${name}' (expected n<number>)" >&2
    exit 1
}

node_count="$(jq '.nodes|length' "${inv}")"

for ((idx = 0; idx < node_count; idx++)); do
    node_name="$(jq -r ".nodes[$idx].name" "${inv}")"
    site_id="$(jq -r ".nodes[$idx].site_id" "${inv}")"
    leader_role="$(jq -r ".nodes[$idx].leader_role" "${inv}")"
    private_ip="$(jq -r ".nodes[$idx].private_ip // .nodes[$idx].local_ip" "${inv}")"
    vlan_if="$(jq -r ".nodes[$idx].vlan_if" "${inv}")"
    wan_port="$(jq -r ".nodes[$idx].wanagent_port" "${inv}")"

    [[ "${leader_role}" == "primary" || "${leader_role}" == "backup" ]] || {
        echo "node[$idx] leader_role must be primary|backup (got ${leader_role})" >&2
        exit 1
    }

    local_id="$(derive_local_id "${node_name}")"

    if [[ "${leader_role}" == "primary" ]]; then
        leader_ip="${primary_ip}"
        leader_port="${primary_port}"
    else
        leader_ip="${backup_ip}"
        leader_port="${backup_port}"
    fi

    if [[ "${site_id}" -eq 0 ]]; then
        is_primary_site="true"
    else
        is_primary_site="false"
    fi

    node_out="${out_dir}/${node_name}"
    mkdir -p "${node_out}"

    # derecho.cfg.tmp
    cp "${main_tmpl}" "${node_out}/derecho.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho.cfg.tmp" "DERECHO" "contact_ip" "${leader_ip}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho.cfg.tmp" "DERECHO" "contact_port" "${leader_port}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho.cfg.tmp" "DERECHO" "local_id" "${local_id}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho.cfg.tmp"

    # derecho_node.cfg.tmp
    cp "${node_tmpl}" "${node_out}/derecho_node.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho_node.cfg.tmp" "DERECHO" "local_ip" "${private_ip}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho_node.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho_node.cfg.tmp" "DERECHO" "local_id" "${local_id}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho_node.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho_node.cfg.tmp" "RDMA" "domain" "${vlan_if}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho_node.cfg.tmp"

    tmp="$(mktemp)"
    patch_section_kv "${node_out}/derecho_node.cfg.tmp" "CASCADE" "is_primary_site" "${is_primary_site}" >"${tmp}"
    mv "${tmp}" "${node_out}/derecho_node.cfg.tmp"

    # wanagent.json
    tmp="$(mktemp)"
    jq -s \
        --argjson local_sid "${site_id}" \
        --arg private_ip "${private_ip}" \
        --argjson private_port "${wan_port}" \
        '
    def sid_of(n):
      # pick the right field; extend this list if your inv uses different names
      (n.site_id // n.site // n.siteId // n.siteID);

    def sid_num(n):
      (sid_of(n) | tostring | tonumber?);

    .[0] as $tpl
    | .[1] as $inv
    | ($inv.nodes) as $nodes

    # map: site_id_number -> {ips:[...], ports:[...]}
    | ( reduce $nodes[] as $n ({}; 
          (sid_num($n)) as $sid
          | if $sid == null then . else
              .[$sid|tostring].ips   += [ (if $sid == $local_sid then $n.private_ip else $n.public_ip end) ]
            | .[$sid|tostring].ports += [ ($n.wanagent_port // $n.wan_port) ]
            end
        )
      ) as $by_site

    | ($inv.sites | map(.id|tonumber?) | map(select(.!=null)) | sort) as $site_ids
    | $tpl
    | .private_ip = $private_ip
    | .private_port = $private_port
    | .local_site_id = $local_sid
    | .sites = (
        $site_ids
        | map({
            id: .,
            ips:   ($by_site[.|tostring].ips   // []),
            ports: ($by_site[.|tostring].ports // [])
        })
      )
  ' \
        "${wan_tmpl}" "${inv}" >"${tmp}"
    mv "${tmp}" "${node_out}/wanagent.json"

    echo "generated: ${node_out}  (local_id=${local_id}, site_id=${site_id}, leader_role=${leader_role})"
done

echo "done."
