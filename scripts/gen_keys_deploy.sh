#!/bin/bash
set -euo pipefail

USER=vsivak
PASS="$HOME/.ssh/scp_pass"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <keys, config or all>"
    echo "Keys to distribute keys and config to distribute config"
    exit 1
fi

eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
# -------- DISTRIBUTION --------

if [ $1 == "config" ] || [ $1 == "all" ]; then
    for n in {0..8}; do
        if [ $n == 4 ]; then
            scp ../scp/n${n}/derecho.cfg.tmp \
                node${n}:~/cascade-cbdc/build/cfg/client/derecho.cfg
            scp ../scp/n${n}/derecho_node.cfg.tmp \
                node${n}:~/cascade-cbdc/build/cfg/client/derecho_node.cfg
            scp ../scp/n${n}/wanagent.json \
                node${n}:~/cascade-cbdc/build/cfg/client/wanagent.json
        else
            scp ../scp/n${n}/derecho.cfg.tmp \
                node${n}:~/cascade-cbdc/build/cfg/n${n}/derecho.cfg
            scp ../scp/n${n}/derecho_node.cfg.tmp \
                node${n}:~/cascade-cbdc/build/cfg/n${n}/derecho_node.cfg
            scp ../scp/n${n}/wanagent.json \
                node${n}:~/cascade-cbdc/build/cfg/n${n}/wanagent.json
        fi
    done

fi
if [ $1 == "keys" ] || [ $1 == "all" ]; then
    # Key Gen
    openssl genpkey -algorithm rsa -outform PEM -out private_key.pem
    openssl pkey -in private_key.pem -pubout -outform PEM -out service_public_key.pem

    openssl genpkey -algorithm rsa -outform PEM -out backup_private_key.pem
    openssl genpkey -algorithm rsa -outform PEM -out client_private_key.pem
    # Servers: shared private key
    for n in {0..3}; do
        scp private_key.pem \
            node${n}:"~/cascade-cbdc/build/cfg/n${n}/private_key.pem"
    done

    # Client: service public key + dummy private key
    scp client_private_key.pem \
        node4:"~/cascade-cbdc/build/cfg/client/private_key.pem"

    scp service_public_key.pem \
        node4:"~/cascade-cbdc/build/cfg/client/service_public_key.pem"

    # Backups: service public key + shared backup private key
    for n in {5..8}; do
        scp backup_private_key.pem \
            node${n}:"~/cascade-cbdc/build/cfg/n${n}/private_key.pem"
        scp service_public_key.pem \
            node${n}:"~/cascade-cbdc/build/cfg/n${n}service_public_key.pem"
    done

    # Cleanup
    rm -f private_key.pem service_public_key.pem backup_private_key.pem client_dummy_private_key.pem
fi
echo "Remote distribution complete."
