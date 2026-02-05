#!/bin/bash

# ==============================================================================
#                          PVM INSTALLATION SCRIPT
# ==============================================================================

echo ""
echo "██████╗ ██╗   ██╗███╗   ███╗    ███╗   ██╗ ██████╗ ██████╗ ███████╗"
echo "██╔══██╗██║   ██║████╗ ████║    ████╗  ██║██╔═══██╗██╔══██╗██╔════╝"
echo "██████╔╝██║   ██║██╔████╔██║    ██╔██╗ ██║██║   ██║██║  ██║█████╗  "
echo "██╔═══╝ ██║   ██║██║╚██╔╝██║    ██║╚██╗██║██║   ██║██║  ██║██╔══╝  "
echo "██║     ╚██████╔╝██║ ╚═╝ ██║    ██║ ╚████║╚██████╔╝██████╔╝███████╗"
echo "╚═╝      ╚═════╝ ╚═╝     ╚═╝    ╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚══════╝"
echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                     PVM INSTALLATION                       ║"
echo "║                     MADE BY WANNYDRAGON                    ║"
echo "║                    VERSION: 2.0.0                          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# ==============================================================================
#                         CONFIGURATION
# ==============================================================================

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
BOLD='\033[1m'
RESET='\033[0m'

# Installation Variables
REPO_URL="https://github.com/pushkarmudganti/PVM-V2"
INSTALL_DIR="/opt/pvm"
VENV_DIR="/opt/pvm-venv"
LOG_DIR="/var/log/pvm"
SERVICE_NAME="pvm"
OS_TYPE=""
OS_VERSION=""
SUDO_USER=""
REBOOT_REQUIRED=false
INSTALL_START_TIME=$(date +%s)

# ==============================================================================
#                         FUNCTIONS
# ==============================================================================

print_status() {
    local status=$1
    local message=$2
    case $status in
        "info") echo -e "${BLUE}[•]${RESET} ${WHITE}${message}${RESET}" ;;
        "success") echo -e "${GREEN}[✓]${RESET} ${GREEN}${message}${RESET}" ;;
        "warning") echo -e "${YELLOW}[!]${RESET} ${YELLOW}${message}${RESET}" ;;
        "error") echo -e "${RED}[✗]${RESET} ${RED}${message}${RESET}" ;;
        "step") echo -e "${PURPLE}[→]${RESET} ${CYAN}${BOLD}${message}${RESET}" ;;
    esac
}

check_root() {
    if [ "$EUID" -ne 0 ]; then 
        print_status "error" "Please run as root (use: sudo ./install.sh)"
        exit 1
    fi
    print_status "success" "Root privileges confirmed"
}

detect_os() {
    print_status "step" "Detecting Operating System"
    
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS_TYPE="${ID}"
        OS_VERSION="${VERSION_ID}"
        OS_NAME="${PRETTY_NAME}"
        
        print_status "success" "Detected: ${WHITE}${OS_NAME}"
        print_status "info" "Version: ${WHITE}${OS_VERSION}"
        
        case "${OS_TYPE}" in
            ubuntu)
                case "${OS_VERSION}" in
                    "20.04"|"22.04"|"24.04")
                        print_status "success" "Supported Ubuntu version"
                        ;;
                    *)
                        print_status "warning" "Ubuntu ${OS_VERSION} - Experimental"
                        ;;
                esac
                ;;
            debian)
                case "${OS_VERSION}" in
                    "11"|"12"|"13")
                        REBOOT_REQUIRED=true
                        print_status "warning" "Debian ${OS_VERSION} detected"
                        print_status "warning" "System will reboot automatically after installation"
                        ;;
                    *)
                        REBOOT_REQUIRED=true
                        print_status "warning" "Debian ${OS_VERSION} detected"
                        print_status "warning" "System will reboot automatically after installation"
                        ;;
                esac
                ;;
            *)
                print_status "error" "Unsupported OS: ${OS_TYPE}"
                exit 1
                ;;
        esac
    else
        print_status "error" "Cannot detect OS"
        exit 1
    fi
}

reboot_countdown() {
    echo ""
    print_status "warning" "⚠ DEBIAN SYSTEM DETECTED - REBOOT REQUIRED"
    print_status "warning" "System will reboot in 10 seconds for LXD to work properly"
    print_status "warning" "Press Ctrl+C to cancel reboot"
    echo ""
    
    for i in {10..1}; do
        echo -ne "${YELLOW}Rebooting in ${i} seconds... ${RESET}\r"
        sleep 1
    done
    
    echo ""
    print_status "info" "Rebooting system now..."
    print_status "info" "After reboot, run: sudo lxd init"
    reboot
}

get_sudo_user() {
    SUDO_USER="${SUDO_USER:-$(logname 2>/dev/null || echo "${USER}")}"
    print_status "info" "User: ${WHITE}${SUDO_USER}"
}

update_system() {
    print_status "step" "Updating System"
    apt-get update -y > /dev/null 2>&1
    apt-get upgrade -y > /dev/null 2>&1
    print_status "success" "System updated"
}

install_dependencies() {
    print_status "step" "Installing Dependencies"
    
    apt-get install -y \
        curl wget git python3 python3-pip python3-venv \
        snapd bridge-utils debootstrap squashfs-tools > /dev/null 2>&1
    
    print_status "success" "Dependencies installed"
}

install_snap_lxd() {
    print_status "step" "Installing Snap and LXD"
    
    # Install snapd if not installed
    if ! command -v snap &> /dev/null; then
        apt-get install -y snapd > /dev/null 2>&1
        print_status "success" "Snap installed"
    fi
    
    # Enable snapd socket
    systemctl enable --now snapd.socket > /dev/null 2>&1
    
    # Install LXD via snap
    print_status "info" "Installing LXD via snap..."
    snap install lxd --classic > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_status "success" "LXD installed via snap"
    else
        print_status "error" "Failed to install LXD"
        exit 1
    fi
    
    print_status "warning" "IMPORTANT: LXD initialization is required"
    print_status "warning" "After installation, run: sudo lxd init"
}

clone_repository() {
    print_status "step" "Cloning Repository"
    
    # Go to /opt directory
    cd /opt
    
    # Remove old if exists
    if [ -d "PVM-V2" ]; then
        rm -rf PVM-V2
        print_status "info" "Removed old PVM-V2 directory"
    fi
    
    # Clone repository
    git clone "${REPO_URL}" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_status "success" "Repository cloned to /opt/PVM-V2"
    else
        print_status "error" "Failed to clone repository"
        exit 1
    fi
}

setup_python_env() {
    print_status "step" "Setting up Python Environment"
    
    # Change to repository directory
    cd /opt/PVM-V2 || {
        print_status "error" "Failed to navigate to /opt/PVM-V2"
        exit 1
    }
    
    print_status "success" "Changed to PVM-V2 directory"
    
    # Create virtual environment
    python3 -m venv "${VENV_DIR}" > /dev/null 2>&1
    source "${VENV_DIR}/bin/activate"
    
    # Upgrade pip
    pip install --upgrade pip > /dev/null 2>&1
    print_status "success" "pip upgraded"
    
    # Install requirements from repository
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt > /dev/null 2>&1
        print_status "success" "Requirements installed from requirements.txt"
    else
        # Install basic packages
        pip install pylxd requests > /dev/null 2>&1
        print_status "info" "Basic packages installed"
    fi
}

create_systemd_service() {
    print_status "step" "Creating Systemd Service"
    
    cat > "/etc/systemd/system/${SERVICE_NAME}.service" << EOF
[Unit]
Description=PVM - LXD/LXC Management Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/PVM-V2
ExecStart=${VENV_DIR}/bin/python3 /opt/PVM-V2/bot.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}.service" > /dev/null 2>&1
    print_status "success" "Systemd service created: ${SERVICE_NAME}"
}

create_directories() {
    print_status "step" "Creating Directories"
    
    mkdir -p "${LOG_DIR}"
    chmod 775 "${LOG_DIR}"
    
    if [ -n "${SUDO_USER}" ]; then
        chown -R "${SUDO_USER}:${SUDO_USER}" /opt/PVM-V2
        chown -R "${SUDO_USER}:${SUDO_USER}" "${LOG_DIR}"
    fi
    
    print_status "success" "Directories created"
}

show_summary() {
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${GREEN}║${RESET}                    INSTALLATION COMPLETE                   ${GREEN}║${RESET}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${RESET}"
    echo ""
    
    echo -e "${WHITE}📋 Installation Summary:${RESET}"
    echo -e "${CYAN}├─${RESET} OS: ${WHITE}${OS_NAME}${RESET}"
    echo -e "${CYAN}├─${RESET} Repository: ${WHITE}/opt/PVM-V2${RESET}"
    echo -e "${CYAN}├─${RESET} Virtual Environment: ${WHITE}${VENV_DIR}${RESET}"
    echo -e "${CYAN}├─${RESET} Systemd Service: ${WHITE}${SERVICE_NAME}${RESET}"
    echo ""
    
    echo -e "${YELLOW}⚠ IMPORTANT NEXT STEPS:${RESET}"
    echo -e "${CYAN}├─${RESET} ${BOLD}1. Initialize LXD:${RESET}"
    echo -e "${CYAN}│  ${RESET}${WHITE}sudo lxd init${RESET}"
    echo -e "${CYAN}│  ${RESET}(Choose default options or customize as needed)"
    echo ""
    echo -e "${CYAN}├─${RESET} ${BOLD}2. Add user to LXD group:${RESET}"
    echo -e "${CYAN}│  ${RESET}${WHITE}sudo usermod -aG lxd \${USER}${RESET}"
    echo -e "${CYAN}│  ${RESET}${WHITE}newgrp lxd${RESET}"
    echo ""
    echo -e "${CYAN}├─${RESET} ${BOLD}3. Start PVM Service:${RESET}"
    echo -e "${CYAN}│  ${RESET}${WHITE}systemctl start ${SERVICE_NAME}${RESET}"
    echo ""
    echo -e "${CYAN}├─${RESET} ${BOLD}4. Check Service Status:${RESET}"
    echo -e "${CYAN}│  ${RESET}${WHITE}systemctl status ${SERVICE_NAME}${RESET}"
    echo ""
    
    if [ "${REBOOT_REQUIRED}" = true ]; then
        echo -e "${RED}🚨 DEBIAN DETECTED - AUTOMATIC REBOOT${RESET}"
        echo -e "${RED}   System will reboot in 10 seconds${RESET}"
        echo -e "${RED}   After reboot, run: sudo lxd init${RESET}"
        echo ""
    else
        echo -e "${GREEN}✅ Ready to continue${RESET}"
        echo ""
    fi
    
    echo -e "${WHITE}🛠 Manual Commands:${RESET}"
    echo -e "${CYAN}├─${RESET} cd /opt/PVM-V2"
    echo -e "${CYAN}├─${RESET} source ${VENV_DIR}/bin/activate"
    echo -e "${CYAN}└─${RESET} python3 bot.py"
    echo ""
    echo -e "${GREEN}Made by WANNYDRAGON${RESET}"
}

# ==============================================================================
#                         MAIN INSTALLATION
# ==============================================================================

# Start
clear

# Check root
check_root

# Detect OS
detect_os

# Get user
get_sudo_user

# Update system
update_system

# Install dependencies
install_dependencies

# Install snap and LXD (NO lxd init)
install_snap_lxd

# Clone repository
clone_repository

# Setup Python environment
setup_python_env

# Create directories
create_directories

# Create systemd service
create_systemd_service

# Show summary
show_summary

# Automatic reboot for Debian
if [ "${REBOOT_REQUIRED}" = true ]; then
    reboot_countdown
fi

# ==============================================================================
#                         END
# ==============================================================================
