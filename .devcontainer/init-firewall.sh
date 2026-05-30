#!/usr/bin/env bash
# Default-deny outbound firewall for the archetype-core-etl devcontainer.
# Allows only the domains the agent chain genuinely needs.

set -euo pipefail

ALLOWED_DOMAINS=(
  "api.anthropic.com"
  "claude.ai"
  "console.anthropic.com"
  "platform.claude.com"
  "statsig.anthropic.com"
  "github.com"
  "api.github.com"
  "codeload.github.com"
  "objects.githubusercontent.com"
  "raw.githubusercontent.com"
  "registry.npmjs.org"
  "pypi.org"
  "files.pythonhosted.org"
)

REQUIRED_DOMAINS=(
  "api.anthropic.com"
  "platform.claude.com"
  "github.com"
  "api.github.com"
  "codeload.github.com"
  "objects.githubusercontent.com"
  "raw.githubusercontent.com"
  "registry.npmjs.org"
  "pypi.org"
  "files.pythonhosted.org"
)

is_required_domain() {
  local domain="$1"

  for required in "${REQUIRED_DOMAINS[@]}"; do
    if [ "${domain}" = "${required}" ]; then
      return 0
    fi
  done

  return 1
}

echo "==> Resetting firewall rules"

iptables -F
iptables -X 2>/dev/null || true
ipset destroy allowed-domains 2>/dev/null || true
ipset create allowed-domains hash:ip

# Allow established/related traffic so responses can return.
iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT

# Loopback is allowed.
iptables -A INPUT -i lo -j ACCEPT
iptables -A OUTPUT -o lo -j ACCEPT

# DNS is allowed so the allowlist can be resolved.
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT

# Allow host gateway for docker-compose services.
HOST_GW=$(ip route | awk '/default/ {print $3; exit}')
if [ -n "${HOST_GW}" ]; then
  echo "==> Allowing host gateway ${HOST_GW} for compose services"
  iptables -A OUTPUT -d "${HOST_GW}" -j ACCEPT
fi

echo "==> Resolving allowlisted domains"

for domain in "${ALLOWED_DOMAINS[@]}"; do
  echo "Resolving ${domain}..."

  ips=$(dig +short A "${domain}" 2>/dev/null | grep -E '^[0-9.]+$' || true)

  if [ -z "${ips}" ]; then
    if is_required_domain "${domain}"; then
      echo "ERROR: Required domain failed to resolve: ${domain}"
      exit 1
    else
      echo "WARN: Optional domain failed to resolve: ${domain}. No allow rule added."
      continue
    fi
  fi

  for ip in ${ips}; do
    echo "Adding ${ip} for ${domain}"
    ipset add allowed-domains "${ip}" 2>/dev/null || true
  done
done

ENTRY_COUNT=$(ipset list allowed-domains | awk '/Number of entries:/ {print $4}')

if [ "${ENTRY_COUNT}" = "0" ]; then
  echo "ERROR: allowed-domains ipset is empty. Refusing to enable default-deny firewall."
  exit 1
fi

# Allow HTTP/HTTPS only to resolved allowlisted destination IPs.
iptables -A OUTPUT -p tcp -m multiport --dports 80,443 -m set --match-set allowed-domains dst -j ACCEPT

# Set default policies last, after required allow rules exist.
iptables -P INPUT DROP
iptables -P FORWARD DROP
iptables -P OUTPUT DROP

OUTPUT_POLICY=$(iptables -S OUTPUT | awk 'NR==1 {print $3}')

if [ "${OUTPUT_POLICY}" != "DROP" ]; then
  echo "ERROR: OUTPUT policy is not DROP. Firewall is not enforcing."
  iptables -S OUTPUT
  exit 1
fi

echo "==> Firewall active. Outbound restricted to allowlist + compose services."