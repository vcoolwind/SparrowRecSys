# /bin/zsh -
docker run -t --rm  --name half_plus_two_cpu \
-p 8502:8501   \
-v "/Users/wangyanfeng/dev_workspace/SparrowRecSys/OnlineServer/src/main/resources/webroot/modeldata/saved_model_half_plus_two_cpu:/models/half_plus_two_cpu"    \
-e MODEL_NAME=half_plus_two_cpu     \
tensorflow/serving &

# curl http://localhost:8502/v1/models/half_plus_two_cpu/metadata
# curl -d'{"instances": [1.0, 2.0, 5.0]}' -X POST http://localhost:8502/v1/models/half_plus_two_cpu:predict
# docker stop half_plus_two_cpu