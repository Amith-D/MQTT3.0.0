#!/bin/bash

echo 'Stopping feedback.service'
sudo systemctl stop feedback.service
echo 'Starting feedback.service'
sudo systemctl start feedback.service

echo 'Stopping update.service'
sudo systemctl stop status_update.service
echo 'Starting update.service'
sudo systemctl start status_update.service