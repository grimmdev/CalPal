#!/bin/bash
set -e

REPO="grimmdev/calpal"

echo "Installing Calpal..."

OS="$(uname -s)"
case "${OS}" in
    Linux*)     OS_NAME=linux;;
    Darwin*)    OS_NAME=darwin;;
    *)          echo "Unsupported OS: ${OS}"; exit 1;;
esac

ARCH="$(uname -m)"
case "${ARCH}" in
    x86_64*)    ARCH_NAME=amd64;;
    aarch64*|arm64*) ARCH_NAME=arm64;;
    *)          echo "Unsupported Architecture: ${ARCH}"; exit 1;;
esac

ARCHIVE_NAME="calpal-${OS_NAME}-${ARCH_NAME}.tar.gz"

echo "Detected system: ${OS_NAME} ${ARCH_NAME}"
echo "Fetching latest release from GitHub..."

LATEST_URL=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep "browser_download_url.*${ARCHIVE_NAME}" | cut -d : -f 2,3 | tr -d \" | xargs)

if [ -z "$LATEST_URL" ]; then
    echo "Error: Could not find a suitable release archive (${ARCHIVE_NAME}) for your system."
    echo "Please check the releases page manually: https://github.com/${REPO}/releases"
    exit 1
fi

echo "Downloading ${ARCHIVE_NAME}..."
curl -sL "$LATEST_URL" -o "$ARCHIVE_NAME"

echo "Extracting archive..."
tar -xzf "$ARCHIVE_NAME"

echo "Cleaning up..."
rm "$ARCHIVE_NAME"

chmod +x calpal

echo ""
echo "Calpal successfully installed in the current directory."
echo "To start the application, run:"
echo ""
echo "  PORT=8080 ./calpal"
echo ""