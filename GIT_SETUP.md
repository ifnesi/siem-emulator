# Git Setup Instructions

Follow these steps to create the repository and push the code to GitHub.

## Step 1: Create Repository on GitHub

1. Go to https://github.com/ifnesi
2. Click the "+" icon in the top right corner
3. Select "New repository"
4. Repository name: `siem-emulator`
5. Description: `SIEM Data Emulator - Generate realistic security data for Kafka with Avro serialization`
6. Choose "Public" or "Private"
7. **DO NOT** initialize with README, .gitignore, or license (we already have these)
8. Click "Create repository"

## Step 2: Initialize Git and Push Code

Run these commands in your terminal from the project directory:

```bash
# Navigate to project directory
cd /Users/inesi/Documents/_CFLT/Dev/Docker/siem-emulator

# Initialize git repository
git init

# Add all files (respects .gitignore)
git add .

# Create initial commit
git commit -m "Initial commit: SIEM Data Emulator with 5 templates

- Python producer with Avro serialization
- 5 comprehensive SIEM templates (dns_log, siem_log, net_device, syslog_log, pcap_data)
- Automatic schema inference and registration
- Dry-run mode for testing
- Docker Compose for Confluent Platform
- Complete documentation"

# Add remote repository
git remote add origin https://github.com/ifnesi/siem-emulator.git

# Push to GitHub
git branch -M main
git push -u origin main
```

## Step 3: Verify

1. Go to https://github.com/ifnesi/siem-emulator
2. Verify all files are present
3. Check that .venv directory is NOT included

## Optional: Add Topics and Description

On GitHub repository page:
1. Click the gear icon next to "About"
2. Add topics: `kafka`, `avro`, `siem`, `confluent`, `python`, `data-generator`, `security`, `splunk`, `kibana`
3. Add website (if applicable)
4. Save changes

## Files Included

The following files will be committed:
- README.md
- requirements.txt
- siem_producer.py
- docker-compose.yml
- .gitignore
- .env
- notes.txt
- kafka/config.properties
- kafka/registry.properties
- templates/dns_log.tpl
- templates/siem_log.tpl
- templates/net_device.tpl
- templates/syslog_log.tpl
- templates/pcap_data.tpl

## Files Excluded (via .gitignore)

- .venv/ (Python virtual environment)
- vol/ (Docker volumes)
- __pycache__/ (Python cache)
- .vscode/ (IDE settings)
- .DS_Store (macOS files)