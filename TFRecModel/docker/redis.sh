# /bin/zsh -
docker run -d --name redis -v `pwd`/redis_data:/data -p 6379:6379 redis:latest
