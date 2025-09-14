#!/bin/bash
# Master Pipeline Script for TeaTrade Data Acquisition

# --- Configuration ---
USER_HOME=$HOME
REPO_DIR="$USER_HOME/robtaylor1203-cmd.github.io"
VENV_PATH="$REPO_DIR/venv/bin/activate"
LOG_FILE="$REPO_DIR/pipeline.log"
# ---------------------

# Function to log messages
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to run a python script
run_python_script() {
    log "Executing: $1"
    # Redirect script output (stdout and stderr) to the log file
    python3 "$1" >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        log "Warning: Error detected running $1. Check log for details."
    fi
}

log "--- Starting TeaTrade Data Pipeline ---"

# Navigate and Activate Environment
cd "$REPO_DIR" || { log "Error: Cannot navigate to $REPO_DIR. Exiting."; exit 1; }
source "$VENV_PATH" || { log "Error: Cannot activate virtual environment. Exiting."; exit 1; }

# Ensure the repository is up-to-date
log "Pulling latest repository changes..."
git pull origin main >> "$LOG_FILE" 2>&1

# --- 1. Data Acquisition ---
# Scripts now generate manifests automatically.
log "Starting Data Acquisition Phase (This may take 20-30 minutes)..."

# 1.1 Kolkata (J Thomas)
run_python_script "scrape_JT_auctionlots.py"
run_python_script "scrape_JT_marketreport.py"
run_python_script "scrape_JT_districtaverages.py"
run_python_script "scrape_JT_synopsis.py"

# 1.2 Colombo (CTB & Forbes)
run_python_script "scrape_CTB_marketreport.py"
run_python_script "scrape_FW_marketreport_direct.py"
run_python_script "scrape_FW_weeklyquantities_ocr.py"
run_python_script "scrape_FW_monthlyexport_ocr.py"

# 1.3 Mombasa (Assumes files are already in Inbox/mombasa)
run_python_script "process_mombasa_inbox.py"

# --- 2. Data Consolidation ---
log "Starting Data Consolidation Phase..."
run_python_script "consolidate_v2.py"

# --- 3. Data Analysis (Pandas) ---
log "Starting Data Analysis (Pandas)..."
run_python_script "analyze_data.py"

# Deactivate the environment
deactivate

# --- 4. Deployment (Git) ---
log "Starting Deployment Phase..."

# Check if there are changes (new source files, library update, analysis output)
if [[ -n $(git status -s) ]]; then
    log "Changes detected. Committing and pushing."
    git add .
    COMMIT_MSG="Automated Pipeline Update: $(date '+%Y-%m-%d %H:%M')"
    git commit -m "$COMMIT_MSG"

    # Push the changes (Relies on cached credentials)
    git push origin main >> "$LOG_FILE" 2>&1
    if [ $? -eq 0 ]; then
        log "Successfully pushed to GitHub."
    else
        log "Error: Failed to push to GitHub. Check credentials (PAT cache) or network."
    fi
else
    log "No new data found. Skipping deployment."
fi

log "--- TeaTrade Data Pipeline Finished ---"
