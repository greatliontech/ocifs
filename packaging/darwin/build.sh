#!/usr/bin/env bash
# Build and code-sign ocifs.app: the host bundle carrying the ocifs
# CLI and the OcifsFS FSKit app extension (cmd/ocifs-fskit). Run on
# macOS 15.4+ with Go and Xcode command-line tools. Mirrors the
# fskit-go validation bundle recipe.
#
# Required environment:
#   SIGN_IDENTITY     codesign identity ("Apple Development: you@example.com (TEAMID)")
#   PROVISION_PROFILE .provisionprofile whose App ID has the FSKit Module
#                     capability and matches the appex bundle id below
#                     (the restricted com.apple.developer.fskit.fsmodule
#                     entitlement requires a PAID Apple Developer team)
#
# The application-group id in both entitlements files
# (group.tech.greatlion.ocifs) must match your team's registered
# group, and the store work directory must live inside that group
# container — the sandboxed extension can open nothing else
# (docs/specs/api.md REQ-api-mount-darwin).
#
# Output: packaging/darwin/build/ocifs.app
set -euo pipefail

cd "$(dirname "$0")/../.."      # repo root
HERE=packaging/darwin
OUT=$HERE/build
APP=$OUT/ocifs.app
APPEX=$APP/Contents/PlugIns/OcifsFS.appex

: "${SIGN_IDENTITY:?set SIGN_IDENTITY (see header)}"
: "${PROVISION_PROFILE:?set PROVISION_PROFILE (see header)}"
[ -f "$PROVISION_PROFILE" ] || { echo "PROVISION_PROFILE not found: $PROVISION_PROFILE" >&2; exit 1; }

echo "==> building binaries (native arch, no cgo)"
rm -rf "$OUT"
mkdir -p "$APPEX/Contents/MacOS" "$APP/Contents/MacOS"
CGO_ENABLED=0 go build -o "$APPEX/Contents/MacOS/OcifsFS" ./cmd/ocifs-fskit
CGO_ENABLED=0 go build -o "$APP/Contents/MacOS/ocifs" ./cmd/ocifs

echo "==> assembling bundle"
cp "$HERE/appex-Info.plist" "$APPEX/Contents/Info.plist"
cp "$HERE/host-Info.plist"  "$APP/Contents/Info.plist"
cp "$PROVISION_PROFILE"     "$APPEX/Contents/embedded.provisionprofile"

echo "==> signing (extension, then host)"
codesign --force --options runtime --timestamp=none \
	--entitlements "$HERE/appex-entitlements.plist" \
	--sign "$SIGN_IDENTITY" "$APPEX"
codesign --force --options runtime --timestamp=none \
	--entitlements "$HERE/host-entitlements.plist" \
	--sign "$SIGN_IDENTITY" "$APP"

echo "==> verifying signatures"
codesign --verify --deep --strict --verbose=2 "$APP"

echo
echo "OK: $APP"
echo "Install: copy to ~/Applications, run"
echo "  /System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f ~/Applications/ocifs.app"
echo "then enable the extension in System Settings > General > Login Items & Extensions > File System Extensions."
