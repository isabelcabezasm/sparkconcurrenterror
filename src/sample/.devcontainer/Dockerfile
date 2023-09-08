# Allow external collaborators to use a public base image instead
ARG PYTHON_BASE_IMAGE
FROM ${PYTHON_BASE_IMAGE}

# Configure apt
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y install --no-install-recommends \
    wget apt-utils \
    # Install git, process tools, lsb-release (common in install instructions for CLIs)
    git procps lsb-release \
    # Install any missing dependencies for enhanced language service
    libicu[0-9][0-9] \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /workspace
WORKDIR /workspace

ENV DEBIAN_FRONTEND=dialog

# Set PACKAGE_VERSION to localdev
ENV PACKAGE_VERSION=localdev

# Set the default shell to bash rather than sh
ENV SHELL /bin/bash
