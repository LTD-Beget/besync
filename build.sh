#!/bin/bash
set -e

rm -fr /root/.ssh/
cp -r /root/ssh  /root/.ssh

chown -R root:root /root/.ssh/

chmod 700 /root/.ssh/
chmod 600 /root/.ssh/id_rsa*

git config --global url."git@gitlab.beget.ru:".insteadOf "https://gitlab.beget.ru/"

exec "$@"
