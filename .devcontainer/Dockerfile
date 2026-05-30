# Devcontainer image for archetype-core-etl.
# Python 3.13 + dev tooling, with the venv living INSIDE the container so
# subprocess Python (pytest, ruff, mypy) runs without sandbox import failures.

FROM mcr.microsoft.com/devcontainers/python:3.13

ARG USERNAME=vscode

# System packages: zsh + fzf for shell, iptables/ipset for the firewall,
# git already present in the base image.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
        zsh \
        fzf \
        iptables \
        ipset \
        dnsutils \
        jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a dedicated venv at a fixed path the devcontainer.json points at.
# Everything Python the agents run lives here, inside the container boundary.
RUN python -m venv /opt/venv \
    && chown -R ${USERNAME}:${USERNAME} /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV VIRTUAL_ENV="/opt/venv"

# Copy the firewall script and make it root-owned and immutable-ish.
COPY .devcontainer/init-firewall.sh /usr/local/bin/init-firewall.sh
RUN chmod 755 /usr/local/bin/init-firewall.sh

# Allow the vscode user to run ONLY the firewall script via sudo without a password.
RUN echo "${USERNAME} ALL=(root) NOPASSWD: /usr/local/bin/init-firewall.sh" \
    > /etc/sudoers.d/init-firewall \
    && chmod 0440 /etc/sudoers.d/init-firewall

USER ${USERNAME}
