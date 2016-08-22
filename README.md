# 1. Network creation for static IPs
```bash
docker network create --subnet=172.16.6.0/24 test-go
```

# 2. Start Kafka and create topics
```bash
export AUTO_CREATE_TOPICS="input,output"
docker run --net test-go --ip "172.16.6.66" spotify/kafka
```
after that add on host machine record in /etc/hosts:
```
172.16.6.66     9cf3d4e5f23f
```
last word - container id
without this record consumer may not work


# 3. Build and run service container
```bash
docker build -t test-go/service service/.
docker run --net test-go --ip "172.16.6.67" -e KAFKA_ADDR=172.16.6.66:9092 test-go/service
```

# 4. Build and run web service container
```bash
docker build -t test-go/endpoint endpoint/.
docker run --net test-go --ip "172.16.6.68" -e KAFKA_ADDR=172.16.6.66:9092  test-go/endpoint
```

# 5. Test requests

success
```bash
curl -v -d "{\"amount\": 0.001, \"address\": \"3HrLaD7sdxByJiYHnjhYtwq5cDBADzU4tC\"}" http://admin:password@172.16.6.68:8000/ && echo ""
```

fail
```bash
curl -v -d "{\"amoiunt\": 0.001, \"address\": \"3HrLaD7sdxByJiYHnjhYtwq5cDBADzU4tC\"}" http://admin:password@172.16.6.68:8000/ && echo ""
```

unauthorized
```bash
curl -v -d "{\"amoiunt\": 0.001, \"address\": \"3HrLaD7sdxByJiYHnjhYtwq5cDBADzU4tC\"}" http://172.16.6.68:8000/ && echo ""
```
