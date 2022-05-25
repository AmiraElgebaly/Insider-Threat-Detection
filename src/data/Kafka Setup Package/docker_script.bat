docker-compose -f docker-compose.yml up -d
timeout /t 5
docker exec kafka /bin/bash -c "cd /opt/kafka/bin; kafka-topics.sh --create --topic logon_logs --bootstrap-server localhost:9092"
docker exec kafka /bin/bash -c "cd /opt/kafka/bin; kafka-topics.sh --create --topic device_logs --bootstrap-server localhost:9092"
docker exec kafka /bin/bash -c "cd /opt/kafka/bin; kafka-topics.sh --create --topic file_logs --bootstrap-server localhost:9092"
docker exec kafka /bin/bash -c "cd /opt/kafka/bin; kafka-topics.sh --create --topic email_logs --bootstrap-server localhost:9092"
docker exec kafka /bin/bash -c "cd /opt/kafka/bin; kafka-topics.sh --create --topic http_logs --bootstrap-server localhost:9092"
docker exec kafka /bin/bash -c "cd /opt/kafka/bin; kafka-topics.sh --create --topic insider_threat-predictions --bootstrap-server localhost:9092"
pip install -r requirements.txt


