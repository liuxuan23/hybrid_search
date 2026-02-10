import os
import csv
import time
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, DatabaseUnavailable
from tqdm import tqdm

"""
将 TSV 文件导入到 Neo4j 数据库。
TSV 格式：head_type, head, relation, tail_type, tail
"""

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")
TRIPLES_TSV_SAMPLE = os.path.join(DATA_DIR, "triples_sample_10k.tsv")

# Neo4j 配置
# 对于单机 Neo4j 实例，使用 bolt:// 协议
# 对于集群环境，使用 neo4j:// 协议
NEO4J_URI = "bolt://localhost:7687"  # 单机实例使用 bolt://
# NEO4J_URI = "neo4j://localhost:7687"  # 集群环境使用 neo4j://
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "b230b230"
NEO4J_DATABASE = "neo4j"

# 批量导入大小
BATCH_SIZE = 1000

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 1  # 秒


def get_database_name(driver):
	"""获取可用的数据库名称"""
	# 尝试使用指定的数据库
	try:
		with driver.session(database=NEO4J_DATABASE) as session:
			session.run("RETURN 1")
			return NEO4J_DATABASE
	except (DatabaseUnavailable, ServiceUnavailable):
		pass
	
	# 尝试使用默认数据库（Neo4j 4.x 使用 None，5.x 使用 "neo4j"）
	try:
		with driver.session() as session:  # 不指定数据库，使用默认
			session.run("RETURN 1")
			return None  # 使用默认数据库
	except:
		pass
	
	# 如果都失败，返回原始值
	return NEO4J_DATABASE


def clear_database(driver, db_name):
	"""清空数据库（可选，谨慎使用）"""
	with driver.session(database=db_name) as session:
		session.run("MATCH (n) DETACH DELETE n")
		print("✅ 已清空数据库")


def import_tsv_to_neo4j(tsv_path: str, clear_existing: bool = False, batch_size: int = BATCH_SIZE):
	"""
	将 TSV 文件导入到 Neo4j。
	
	Args:
		tsv_path: TSV 文件路径
		clear_existing: 是否清空现有数据
		batch_size: 批量处理大小
	"""
	if not os.path.exists(tsv_path):
		raise FileNotFoundError(f"未找到文件: {tsv_path}")
	
	driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
	
	try:
		# 测试连接
		try:
			driver.verify_connectivity()
			print(f"✅ 已连接到 Neo4j: {NEO4J_URI}")
		except Exception as e:
			print(f"❌ 无法连接到 Neo4j: {NEO4J_URI}")
			print(f"   错误信息: {e}")
			print(f"\n💡 请检查：")
			print(f"   1. Neo4j 服务是否正在运行？")
			print(f"   2. 连接地址和端口是否正确？")
			print(f"   3. 用户名和密码是否正确？")
			print(f"   4. 如果是单机实例，请使用 bolt:// 协议")
			print(f"   5. 如果是集群环境，请使用 neo4j:// 协议")
			raise
		
		# 确定可用的数据库名称
		db_name = get_database_name(driver)
		if db_name:
			print(f"📌 使用数据库: {db_name}")
		else:
			print(f"📌 使用默认数据库")
		
		# 清空数据库（如果需要）
		if clear_existing:
			clear_database(driver, db_name)
		
		# 读取 TSV 文件并统计行数
		with open(tsv_path, "r", encoding="utf-8") as f:
			reader = csv.reader(f, delimiter="\t")
			header = next(reader)  # 跳过表头
			print(f"表头: {header}")
			
			# 统计总行数
			rows = list(reader)
			total_rows = len(rows)
			print(f"📊 共 {total_rows} 条三元组待导入")
		
		# 创建索引以加速查询
		with driver.session(database=db_name) as session:
			print("📝 创建索引...")
			for retry in range(MAX_RETRIES):
				try:
					session.run("CREATE INDEX entity_id_index IF NOT EXISTS FOR (n:Entity) ON (n.id)")
					session.run("CREATE INDEX entity_type_index IF NOT EXISTS FOR (n:Entity) ON (n.type)")
					# 为关系类型创建索引（Neo4j 5+ 支持关系属性索引）
					try:
						session.run("CREATE INDEX rel_type_index IF NOT EXISTS FOR ()-[r:REL]-() ON (r.type)")
					except:
						pass  # 如果版本不支持关系属性索引，忽略
					print("✅ 索引创建完成")
					break
				except (DatabaseUnavailable, ServiceUnavailable) as e:
					if retry < MAX_RETRIES - 1:
						print(f"⚠️  索引创建重试 ({retry + 1}/{MAX_RETRIES})...")
						time.sleep(RETRY_DELAY)
					else:
						print(f"⚠️  索引创建警告: {e} (可能已存在或数据库不可用)")
				except Exception as e:
					print(f"⚠️  索引创建警告: {e} (可能已存在)")
					break
		
		# 批量导入
		processed = 0
		errors = 0
		
		with tqdm(total=total_rows, desc="导入进度") as pbar:
			for i in range(0, total_rows, batch_size):
				batch = rows[i:i + batch_size]
				
				# 构建批量导入的 Cypher 查询
				# 使用 MERGE 创建节点和关系，关系类型为 REL，type 属性存储关系名称
				# 与 benchmark_lancedb_vs_neo4j.py 中的查询格式兼容
				query = """
				UNWIND $batch AS row
				MERGE (head:Entity {id: row.head})
				ON CREATE SET head.type = CASE WHEN row.head_type <> '' THEN row.head_type ELSE 'Entity' END
				MERGE (tail:Entity {id: row.tail})
				ON CREATE SET tail.type = CASE WHEN row.tail_type <> '' THEN row.tail_type ELSE 'Entity' END
				MERGE (head)-[r:REL {type: row.relation}]->(tail)
				"""
				
				# 使用事务批量处理，带重试机制
				success = False
				for retry in range(MAX_RETRIES):
					try:
						with driver.session(database=db_name) as session:
							session.run(query, batch=[
								{
									"head_type": row[0],
									"head": row[1],
									"relation": row[2],
									"tail_type": row[3],
									"tail": row[4]
								}
								for row in batch
							])
						processed += len(batch)
						success = True
						break
					except (DatabaseUnavailable, ServiceUnavailable) as e:
						if retry < MAX_RETRIES - 1:
							time.sleep(RETRY_DELAY * (retry + 1))  # 指数退避
						else:
							print(f"\n❌ 批量导入错误 (行 {i}-{i+len(batch)}): {e}")
							errors += len(batch)
					except Exception as e:
						print(f"\n❌ 批量导入错误 (行 {i}-{i+len(batch)}): {e}")
						errors += len(batch)
						break
				
				pbar.update(len(batch))
		
		# 统计结果
		node_count = rel_count = 0
		for retry in range(MAX_RETRIES):
			try:
				with driver.session(database=db_name) as session:
					node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
					rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
				break
			except (DatabaseUnavailable, ServiceUnavailable) as e:
				if retry < MAX_RETRIES - 1:
					time.sleep(RETRY_DELAY)
				else:
					print(f"⚠️  无法统计结果: {e}")
		
		print(f"\n✅ 导入完成!")
		print(f"  处理行数: {processed}")
		print(f"  错误行数: {errors}")
		print(f"  节点总数: {node_count}")
		print(f"  关系总数: {rel_count}")
		
	except Exception as e:
		print(f"❌ 导入失败: {e}")
		raise
	finally:
		driver.close()


def main():
	import argparse
	
	parser = argparse.ArgumentParser(description="将 TSV 文件导入到 Neo4j")
	parser.add_argument(
		"--file",
		type=str,
		default=TRIPLES_TSV_SAMPLE,
		help=f"TSV 文件路径 (默认: {TRIPLES_TSV_SAMPLE})"
	)
	parser.add_argument(
		"--clear",
		action="store_true",
		help="导入前清空数据库"
	)
	parser.add_argument(
		"--batch-size",
		type=int,
		default=BATCH_SIZE,
		help=f"批量处理大小 (默认: {BATCH_SIZE})"
	)
	
	args = parser.parse_args()
	
	import_tsv_to_neo4j(args.file, clear_existing=args.clear, batch_size=args.batch_size)


if __name__ == "__main__":
	main()

