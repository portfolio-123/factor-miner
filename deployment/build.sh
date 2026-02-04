docker build -t factor-eval -f deploy.Dockerfile ../

IMAGE=factor-eval
TAG=latest
REGISTRY=quay300.portfolio123.com/misc

docker image tag $IMAGE:$TAG $REGISTRY/$IMAGE:$TAG
docker image push $REGISTRY/$IMAGE:$TAG
