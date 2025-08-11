#!/bin/sh

set -e

IMAGE_NAME=wowaudit-bot-image
DOCKER_BUILDKIT=1 docker build -t $IMAGE_NAME .

CONTAINER_NAME=wowaudit-bot

if [ "$(docker context show)" == "chookity" ]; then
  CONFIG_FILE=./configs/bots_chookity.yaml
else
  CONFIG_FILE=./configs/bots_local.yaml
fi

if docker container ls -a | grep -q $CONTAINER_NAME; then
  echo "Removing previous container"
  docker rm -f $CONTAINER_NAME > /dev/null
fi

docker run \
  --name $CONTAINER_NAME \
  --init \
  -it \
  -d \
  --restart=unless-stopped \
  $IMAGE_NAME \
  ./wowaudit_bot.py --loop --config_file $CONFIG_FILE --expansion TWW --season S3
