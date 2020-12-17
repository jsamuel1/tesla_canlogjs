#!/bin/sh

mkdir -p /opt/canlogpi/
cp can_capture.py /opt/canlogpi/
cp can_upload.py /opt/canlogpi/

systemctl disable canlogpi.service || true
systemctl disable canlogpi_upload.service || true

cat << EOF > /lib/systemd/system/canlogpi.service
[Unit]
Description=Capture CAN messages from car
DefaultDependencies=no
After=mutable.mount
[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/python3 /opt/canlogpi/can_capture.py
WorkingDirectory=/backingfiles/canlog
[Install]
WantedBy=multi-user.target
EOF

cat << EOF > /lib/systemd/system/canlogpi_upload.service
[Unit]
Description=Upload captured CAN messages from car
DefaultDependencies=no
After=mutable.mount
[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/python3 /opt/canlogpi/can_upload.py
WorkingDirectory=/backingfiles/canlog
[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable canlogpi.service
systemctl enable canlogpi_upload.service