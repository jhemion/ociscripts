#!/usr/bin/env bash

set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
WORK_ROOT="${WORK_ROOT:-/usr/local/src}"
SOURCE_DIR="${SOURCE_DIR:-${WORK_ROOT}/lustre-client}"
STEP_PAUSE_SECONDS="${STEP_PAUSE_SECONDS:-2}"
INSTALL_METHOD="${INSTALL_METHOD:-auto}"

log() {
  printf '[%s] %s\n' "$SCRIPT_NAME" "$*"
}

die() {
  printf '[%s] ERROR: %s\n' "$SCRIPT_NAME" "$*" >&2
  exit 1
}

pause_between_steps() {
  local reason="${1:-next step}"

  if [[ "${STEP_PAUSE_SECONDS}" =~ ^[0-9]+$ ]] && (( STEP_PAUSE_SECONDS > 0 )); then
    log "Pausing ${STEP_PAUSE_SECONDS}s before ${reason}"
    sleep "${STEP_PAUSE_SECONDS}"
  fi
}

usage() {
  cat <<'EOF'
Build and install the OCI-documented Lustre client on Oracle Linux 8 or 9.

Usage:
  sudo ./install_lustre_client_oci.sh

Environment overrides:
  WORK_ROOT    Root working directory. Default: /usr/local/src
  SOURCE_DIR   Lustre source checkout directory. Default: $WORK_ROOT/lustre-client
  STEP_PAUSE_SECONDS  Seconds to pause between major steps. Default: 2
  INSTALL_METHOD  auto|make|rpm. Default: auto

What the script does:
  1. Verifies Oracle Linux 8 or 9 on x86_64.
  2. Verifies the system is booted into RHCK, or installs RHCK and sets it as default.
  3. Enables the Oracle repositories called out in the OCI Lustre client docs.
  4. Clones the upstream Lustre source and checks out the OCI-documented tag:
       OL8 -> 2.15.5
       OL9 -> 2.15.90
  5. Builds the client RPMs and DKMS RPM.
  6. Installs the client on the same host with 'make install' by default, or via RPMs
     when INSTALL_METHOD=rpm, then runs depmod and loads the module.

If the script has to switch the system from UEK to RHCK, it exits after setting the
default kernel so you can reboot and rerun it.
EOF
}

require_root() {
  if [[ ${EUID} -ne 0 ]]; then
    die "run this script as root"
  fi
}

detect_platform() {
  [[ -r /etc/os-release ]] || die "/etc/os-release not found"
  # shellcheck disable=SC1091
  source /etc/os-release

  OS_ID="${ID:-}"
  OS_VERSION_ID="${VERSION_ID:-}"
  OS_MAJOR="${OS_VERSION_ID%%.*}"
  ARCH="$(uname -m)"

  [[ "${OS_ID}" == "ol" ]] || die "this script only supports Oracle Linux"
  [[ "${ARCH}" == "x86_64" ]] || die "OCI documents Lustre clients for Oracle Linux x86_64; found ${ARCH}"

  case "${OS_MAJOR}" in
    8)
      LUSTRE_TAG="2.15.5"
      BUILD_PACKAGES=(
        git
        libtool
        patch
        pkgconfig
        libnl3-devel.x86_64
        libblkid-devel
        libuuid-devel
        rpm-build
        kernel-rpm-macros
        kernel-devel
        kernel-abi-whitelists
        libmount
        libmount-devel
        libyaml-devel
      )
      ;;
    9)
      LUSTRE_TAG="2.15.90"
      BUILD_PACKAGES=(
        git
        libtool
        patch
        pkgconfig
        libnl3-devel.x86_64
        libblkid-devel
        libuuid-devel
        rpm-build
        kernel-rpm-macros
        kernel-devel
        libmount
        libmount-devel
        libyaml-devel
      )
      ;;
    *)
      die "unsupported Oracle Linux version: ${OS_VERSION_ID}"
      ;;
  esac

  INSTALL_PACKAGES=(dkms libmount-devel libyaml-devel)
}

ensure_tools() {
  if ! rpm -q dnf-plugins-core >/dev/null 2>&1; then
    log "Installing dnf-plugins-core for config-manager support"
    dnf install -y dnf-plugins-core
  fi
}

enable_repositories() {
  log "Enabling required Oracle Linux repositories for OL${OS_MAJOR}"
  dnf config-manager --set-enabled "ol${OS_MAJOR}_codeready_builder"
  dnf config-manager --enable "ol${OS_MAJOR}_developer_EPEL"
  yum-config-manager --enable "ol${OS_MAJOR}_developer"
}

install_rhck_if_needed() {
  if compgen -G '/boot/vmlinuz-*' | grep -q .; then
    if ls /boot/vmlinuz-* 2>/dev/null | grep -qv 'uek'; then
      return 0
    fi
  fi

  log "Installing RHCK packages"
  dnf install -y kernel kernel-core kernel-modules kernel-modules-extra || dnf install -y kernel
}

ensure_running_rhck() {
  local running_kernel default_kernel target_rhck

  running_kernel="$(uname -r)"
  if [[ "${running_kernel}" != *uek* ]]; then
    log "System is already running RHCK: ${running_kernel}"
    return 0
  fi

  log "System is running UEK: ${running_kernel}"
  install_rhck_if_needed

  target_rhck="$(ls /boot/vmlinuz-* | grep -v 'uek' | sort -V | tail -1 || true)"
  [[ -n "${target_rhck}" ]] || die "unable to locate an installed RHCK kernel under /boot"

  default_kernel="$(grubby --default-kernel)"
  if [[ "${default_kernel}" != "${target_rhck}" ]]; then
    log "Setting default boot kernel to RHCK: ${target_rhck}"
    grubby --set-default "${target_rhck}"
  else
    log "Default boot kernel already points to RHCK: ${target_rhck}"
  fi

  cat <<EOF

RHCK is required before building or installing the Lustre client.
The default kernel has been set to:
  ${target_rhck}

Reboot the instance, confirm that 'uname -r' no longer contains 'uek',
and rerun ${SCRIPT_NAME}.
EOF
  exit 100
}

install_build_dependencies() {
  log "Installing build dependencies"
  dnf install -y "${BUILD_PACKAGES[@]}"
}

prepare_source_tree() {
  mkdir -p "${WORK_ROOT}"

  if [[ -d "${SOURCE_DIR}/.git" ]]; then
    log "Refreshing existing Lustre source checkout in ${SOURCE_DIR}"
    git -C "${SOURCE_DIR}" fetch --tags --force origin
  elif [[ -e "${SOURCE_DIR}" ]]; then
    die "source path exists but is not a git checkout: ${SOURCE_DIR}"
  else
    log "Cloning Lustre source into ${SOURCE_DIR}"
    git clone https://github.com/lustre/lustre-release.git "${SOURCE_DIR}"
  fi

  log "Checking out Lustre tag ${LUSTRE_TAG}"
  git -C "${SOURCE_DIR}" checkout "tags/${LUSTRE_TAG}"
}

build_client() {
  log "Building Lustre client and RPMs"
  pushd "${SOURCE_DIR}" >/dev/null
  sh autogen.sh
  ./configure --enable-client
  make
  make rpms
  make dkms-rpm
  popd >/dev/null
}

install_client_dependencies() {
  log "Installing DKMS/runtime dependencies"
  dnf install -y "${INSTALL_PACKAGES[@]}"
}

show_generated_rpms() {
  local -a rpm_roots

  rpm_roots=(
    "${SOURCE_DIR}"
    "${SOURCE_DIR}/build"
    "${SOURCE_DIR}/build/rpm"
    "${SOURCE_DIR}/rpmbuild"
    "${HOME}/rpmbuild"
    "${WORK_ROOT}"
  )

  log "Generated binary RPMs discovered after build:"
  find "${rpm_roots[@]}" -type f -name '*.rpm' ! -name '*.src.rpm' 2>/dev/null | sort -u || true
}

find_rpms() {
  local -a search_roots
  local client_rpm dkms_rpm

  search_roots=(
    "${SOURCE_DIR}"
    "${SOURCE_DIR}/build"
    "${SOURCE_DIR}/build/rpm"
    "${SOURCE_DIR}/rpmbuild"
    "${HOME}/rpmbuild"
    "${WORK_ROOT}"
  )

  client_rpm="$(
    find "${search_roots[@]}" -type f \
      \( -name 'lustre-client-[0-9]*.x86_64.rpm' -o -name 'lustre-client-[0-9]*.aarch64.rpm' -o -name 'lustre-client-[0-9]*.noarch.rpm' -o -name 'kmod-lustre-client-[0-9]*.x86_64.rpm' \) \
      ! -name '*.src.rpm' \
      ! -name '*debuginfo*' \
      ! -name '*debugsource*' 2>/dev/null | sort -V | tail -1
  )"

  dkms_rpm="$(
    find "${search_roots[@]}" -type f \
      \( -name 'lustre-client-dkms-[0-9]*.noarch.rpm' -o -name 'lustre-client-dkms-[0-9]*.x86_64.rpm' \) \
      ! -name '*.src.rpm' \
      ! -name '*debuginfo*' \
      ! -name '*debugsource*' 2>/dev/null | sort -V | tail -1
  )"

  if [[ -z "${client_rpm}" || -z "${dkms_rpm}" ]]; then
    log "Unable to locate the expected Lustre client RPMs. Nearby RPMs:"
    find "${search_roots[@]}" -type f -name '*.rpm' ! -name '*.src.rpm' 2>/dev/null | sort -u >&2 || true
    die "expected Lustre client and Lustre client DKMS RPMs somewhere under ${SOURCE_DIR} or ${WORK_ROOT}"
  fi

  RPM_FILES=("${client_rpm}" "${dkms_rpm}")
  log "Found client RPM: ${client_rpm}"
  log "Found DKMS RPM:   ${dkms_rpm}"
}

install_client_rpms() {
  log "Installing built Lustre client RPMs"
  yum localinstall -y "${RPM_FILES[@]}"
}

install_with_make_install() {
  log "Installing Lustre client on the build host with make install"
  pushd "${SOURCE_DIR}" >/dev/null
  make install
  popd >/dev/null
}

install_built_client() {
  case "${INSTALL_METHOD}" in
    auto)
      if install_with_make_install; then
        return 0
      fi

      log "make install failed; falling back to local RPM installation"
      find_rpms
      install_client_rpms
      ;;
    make)
      install_with_make_install
      ;;
    rpm)
      find_rpms
      install_client_rpms
      ;;
    *)
      die "unsupported INSTALL_METHOD '${INSTALL_METHOD}'; use auto, make, or rpm"
      ;;
  esac
}

activate_module() {
  log "Running depmod and loading Lustre module"
  depmod
  if ! modprobe lustre; then
    dkms status || true
    find /lib/modules/"$(uname -r)" -type f 2>/dev/null | grep lustre || true
    die "lustre kernel module is still unavailable for the running kernel $(uname -r)"
  fi
  lsmod | grep lustre >/dev/null
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_root
  detect_platform
  ensure_tools
  pause_between_steps "repository configuration"
  enable_repositories
  pause_between_steps "kernel validation"
  ensure_running_rhck
  pause_between_steps "dependency installation"
  install_build_dependencies
  pause_between_steps "source preparation"
  prepare_source_tree
  pause_between_steps "client build"
  build_client
  pause_between_steps "RPM summary"
  show_generated_rpms
  pause_between_steps "runtime dependency installation"
  install_client_dependencies
  pause_between_steps "client installation"
  install_built_client
  pause_between_steps "module activation"
  activate_module

  cat <<EOF

Lustre client build and installation completed.

Installed source tag: ${LUSTRE_TAG}
Source directory:      ${SOURCE_DIR}

The OCI documentation recommends rebooting after installation.
EOF
}

main "$@"
