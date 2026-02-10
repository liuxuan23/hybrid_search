# 完整的测试流程
sudo systemctl stop neo4j
sudo rm -rf /var/lib/neo4j/data/databases/neo4j/*
sudo rm -rf /var/lib/neo4j/data/transactions/neo4j/*
sudo systemctl start neo4j

# 等待 Neo4j 完全启动
sleep 5

# 测试前记录初始大小
du -sh /var/lib/neo4j/data/databases/neo4j/

# 运行测试
python benchmark_write_performance.py

# 测试后记录大小
du -sh /var/lib/neo4j/data/databases/neo4j/
du -sh /var/lib/neo4j/data/transactions/neo4j/