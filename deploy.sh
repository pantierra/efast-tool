#!/bin/bash
set -e

MODE="${1:-setup}"
SERVER="${2:-root@49.12.2.88}"
APP_DIR="/opt/satellite-fusion"
DATA_DIR="$APP_DIR/data"

case "$MODE" in
    setup)
        echo "Deploying to $SERVER..."
        TEMP_DIR=$(mktemp -d)
        rsync -av --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' --exclude='data/' --exclude='.env' . "$TEMP_DIR/"
        cat > "$TEMP_DIR/.env.example" <<EOF
CDSE_USER=your_username_here
CDSE_PASSWORD=your_password_here
EOF
        ssh $SERVER "mkdir -p $APP_DIR"
        rsync -av --delete "$TEMP_DIR/" "$SERVER:$APP_DIR/"
        rm -rf "$TEMP_DIR"

        ssh $SERVER <<ENDSSH
set -e
cd $APP_DIR

# Find/install Python 3.11
if ! command -v python3.11 &> /dev/null; then
    apt-get update -qq
    apt-get install -y python3.11 python3.11-venv python3.11-dev 2>/dev/null || {
        apt-get install -y -t trixie-backports python3.11 python3.11-venv python3.11-dev 2>/dev/null || {
            apt-get install -y software-properties-common
            add-apt-repository -y ppa:deadsnakes/ppa 2>/dev/null || true
            apt-get update -qq
            apt-get install -y python3.11 python3.11-venv python3.11-dev
        }
    }
fi

# Setup venv
[ -d venv ] && rm -rf venv
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install git+https://github.com/DHI-GRAS/efast.git -q

# Setup .env
[ ! -f .env ] && [ -f .env.example ] && cp .env.example .env

# Setup systemd service
if [ -f satellite-fusion-web.service ]; then
    sed "s|/opt/satellite-fusion|$APP_DIR|g" satellite-fusion-web.service | \
        sed "s|--directory /opt/satellite-fusion|--directory $APP_DIR/webapp|g" > /tmp/satellite-fusion-web.service
    cp /tmp/satellite-fusion-web.service /etc/systemd/system/
    systemctl daemon-reload
fi

# Create data directory
mkdir -p $DATA_DIR
ENDSSH
        echo "Setup complete!"
        ;;
    
    upload)
        echo "Uploading data to $SERVER..."
        rsync -avh --progress --exclude='*.pyc' --exclude='__pycache__' data/ "$SERVER:$DATA_DIR/"
        echo "Data upload complete!"
        ;;
    
    *)
        echo "Usage: $0 {setup|upload} [server]"
        echo "  setup  - Deploy code and setup server (default)"
        echo "  upload - Upload data directory only"
        exit 1
        ;;
esac
