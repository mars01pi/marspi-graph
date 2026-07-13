#!/bin/bash

# 定义变量便于修改
CONTAINER_NAME="mysql-development"
ROOT_PASSWORD="123456"
MYSQL_VERSION="8.0"

# 如果容器已存在，先停止并删除旧容器（可选，按需释放）
docker stop $CONTAINER_NAME 2>/dev/null
docker rm $CONTAINER_NAME 2>/dev/null

# 执行启动
docker run -d \
  --name $CONTAINER_NAME \
  --restart always \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD="$ROOT_PASSWORD" \
  -e TZ=Asia/Shanghai \
  -v $(pwd)/data:/var/lib/mysql \
  -v $(pwd)/logs:/var/log/mysql \
  -v $(pwd)/conf:/etc/mysql/conf.d \
  mysql:$MYSQL_VERSION \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_unicode_ci \
  --default-authentication-plugin=mysql_native_password

echo "MySQL $MYSQL_VERSION container is starting..."