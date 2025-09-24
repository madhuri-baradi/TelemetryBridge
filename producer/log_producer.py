import time, random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

endpoints = ["/api/user", "/api/order", "/api/payment"]
levels = ["INFO", "ERROR"]

while True:
    endpoint = random.choices(endpoints, weights=[0.7, 0.2, 0.1])[0]
    level = random.choices(levels, weights=[0.8, 0.2])[0]
    message = "OK" if level == "INFO" else "NullPointerException"
    log = f"[2025-09-20 19:45:01] {level} {endpoint} {message}"
    producer.send("raw-logs", log.encode("utf-8"))
    print("Produced:", log)
    time.sleep(0.5)
