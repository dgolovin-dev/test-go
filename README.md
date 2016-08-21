# 1. создание сети
```bash
docker network create --subnet=172.16.6.0/24 test-go
```

# 2. запуск кафки и создание топиков
```bash
export AUTO_CREATE_TOPICS="input,output"
docker run -e --net test-go --ip "172.16.6.66" spotify/kafka
```
после этого может потребоваться добавить запись в /etc/hosts:
```
172.16.6.66     9cf3d4e5f23f
```
последнее слово - это id контейнера, без этого консьюмер не будет работать


# 3. сборка и запуск контейнера сервера обработки
```bash
docker build -t test-go/service service/.
docker run --net test-go --ip "172.16.6.67" -e KAFKA_ADDR=172.16.6.66:9092 test-go/service
```

# 4. сборка и запуск контейнера веб сервера
```bash
docker build -t test-go/endpoint endpoint/.
docker run --net test-go --ip "172.16.6.68" -e KAFKA_ADDR=172.16.6.66:9092  test-go/endpoint
```

# 5. тестовый запрос
```bash
curl -v -d "{\"amount\": 0.001, \"address\": \"3HrLaD7sdxByJiYHnjhYtwq5cDBADzU4tC\"}" http://admin:password@172.16.6.68:8000/ && echo ""
```