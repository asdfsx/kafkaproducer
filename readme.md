配置文件
```
etc/proucer.conf
```

启动程序
```
cd kafkaproducer
python cmd/kafkaproducer.py etc/producer.conf
```

使用 supervisord 启动
```
cd kafkaproducer
supervisord
```