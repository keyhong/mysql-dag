cp /tmp/context-files/pip.conf /etc/pip.conf

VENV_DIR=/tmp/assignments/.venv
if test -d "$VENV_DIR"; then
    rm -rf "$VENV_DIR"
fi
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

pip3 install pip --upgrade
pip3 install -r /tmp/context-files/requirements.txt

pip3 install PyMySQL==1.0.2
pip3 install psutil==5.9.1