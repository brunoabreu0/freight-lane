# Prerequisites — Installation Guide

This guide helps you install everything needed for the Freight Lane workshop hands-on lab.
Please complete these steps **before arriving** at the workshop.

---

## Required Software

| Tool | Purpose | Minimum Version |
|------|---------|----------------|
| **Docker Desktop** | Runs the Kafka cluster and banking pipeline as containers | 4.30+ (Compose v2.20+) |
| **Git** | Clones the workshop repository | 2.30+ |
| **Terminal** | Executes commands | Any (macOS Terminal, iTerm2, Windows Terminal, VS Code) |

---

## Installing Docker Desktop

Docker Desktop includes both Docker Engine and Docker Compose. It supports Intel/AMD (x86)
and Apple Silicon (ARM64) machines.

### macOS

1. Go to [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
2. Download **Docker Desktop for Mac**.
   - The installer automatically detects your chip (Intel or Apple Silicon).
3. Open the `.dmg` file and drag Docker to your Applications folder.
4. Launch Docker Desktop from Applications.
5. Wait for the Docker icon in the menu bar to show "Docker Desktop is running."

**Verify:**
```bash
docker --version
# Expected: Docker version 27.x.x or later

docker compose version
# Expected: Docker Compose version v2.x.x or later
```

### Windows

1. Go to [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
2. Download **Docker Desktop for Windows**.
3. Run the installer. Accept the default settings.
4. If prompted, enable **WSL 2** (Windows Subsystem for Linux). Docker Desktop will guide you.
5. Restart your computer if required.
6. Launch Docker Desktop.

**Verify (PowerShell or Windows Terminal):**
```powershell
docker --version
docker compose version
```

### Linux (Ubuntu/Debian)

```bash
# Remove old versions
sudo apt-get remove docker docker-engine docker.io containerd runc

# Install prerequisites
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

# Add Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add the repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine + Compose
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add your user to the docker group (so you don't need sudo)
sudo usermod -aG docker $USER
# Log out and back in for this to take effect
```

**Verify:**
```bash
docker --version
docker compose version
```

---

## Installing Git

### macOS

Git is pre-installed on most macOS systems. If not:

```bash
# Option 1: Xcode Command Line Tools (recommended)
xcode-select --install

# Option 2: Homebrew
brew install git
```

### Windows

1. Download from [git-scm.com/download/win](https://git-scm.com/download/win)
2. Run the installer. Accept defaults.
3. Git Bash will be available as a terminal option.

### Linux

```bash
sudo apt-get install -y git
```

**Verify:**
```bash
git --version
# Expected: git version 2.x.x
```

---

## Docker Desktop Settings

For the best workshop experience, ensure Docker Desktop has adequate resources:

| Setting | Recommended |
|---------|------------|
| **Memory (RAM)** | ≥ 4 GB |
| **CPUs** | ≥ 2 |
| **Disk** | ≥ 10 GB free |

To check/change: Docker Desktop → Settings → Resources.

---

## Verification Script

Run all checks at once:

```bash
echo "=== Docker ==="
docker --version
docker compose version
echo ""
echo "=== Git ==="
git --version
echo ""
echo "=== Docker Health ==="
docker run --rm hello-world
echo ""
echo "=== All OK ==="
echo "Your environment is ready for the Freight Lane workshop!"
```

If all commands succeed and you see "Hello from Docker!", you're ready.

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `docker: command not found` | Docker Desktop is not installed or not in PATH. Reinstall. |
| `docker compose version` shows v1.x | You have the old standalone `docker-compose`. Update Docker Desktop to get Compose v2. |
| `Cannot connect to the Docker daemon` | Docker Desktop is not running. Launch it from Applications/Start Menu. |
| `docker run hello-world` fails with permission error | On Linux: add your user to the `docker` group (`sudo usermod -aG docker $USER`) and log out/in. |
| Slow performance on macOS | Increase Docker Desktop memory to 6+ GB in Settings → Resources. |
| WSL2 not available on Windows | Enable WSL2: `wsl --install` in an admin PowerShell, then restart. |

---

## Need Help?

If you're having trouble with the setup, arrive 15 minutes early on the day of the workshop — facilitators will be available to help.
