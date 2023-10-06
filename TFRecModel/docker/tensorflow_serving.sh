# /bin/zsh -
docker run -t --rm  --name recmodel \
-p 8501:8501    \
-v "/Users/wangyanfeng/dev_workspace/SparrowRecSys/OnlineServer/src/main/resources/webroot/modeldata/neuralcf:/models/recmodel"    \
-e MODEL_NAME=recmodel     \
tensorflow/serving &


# curl http://localhost:8501/v1/models/recmodel/metadata
# docker stop recmodel