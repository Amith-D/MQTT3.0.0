#!/bin/bash

echo 'Publishing dummy data'
mosquitto_pub -t '/proto/out' -u 'arjun' -P 'qzense' -m '1, 2, 3, 4, 5, 6, 7, BLR_1, DEV_1'
mosquitto_pub -t '/proto/out' -u 'arjun' -P 'qzense' -m '1, 2, 3, 4, 5, 6, 7, BLR_1, DEV_1'
echo 'Data published'