import os
import csv

"""
将 data/huggingkg_tiny/triples.txt 转换为带表头的 TSV，包含实体类型列，便于 Neo4j 导入。
输出：
- data/huggingkg_tiny/triples.tsv               （全量，行数同 triples.txt）
- data/huggingkg_tiny/triples_sample_10k.tsv    （前 10k 行样本，便于快速演示）
"""

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TXT = os.path.join(DATA_DIR, "triples.txt")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")
TRIPLES_TSV_SAMPLE = os.path.join(DATA_DIR, "triples_sample_10k.tsv")

def split_prefix(entity_id: str):
	"""
	将像 'user:xxx' 这样的 ID 拆成 (type, id)
	若无前缀，则 type 为空字符串。
	"""
	if ":" in entity_id:
		prefix, rest = entity_id.split(":", 1)
		return prefix, entity_id  # id 保留完整（含前缀），便于唯一性与回溯
	return "", entity_id

def convert_file(src_path: str, dst_path: str, max_rows: int = None):
	os.makedirs(os.path.dirname(dst_path), exist_ok=True)

	with open(src_path, "r", encoding="utf-8") as fin, \
	     open(dst_path, "w", encoding="utf-8", newline="") as fout:
		writer = csv.writer(fout, delimiter="\t")
		# 表头：包含实体类型与完整 ID
		writer.writerow(["head_type", "head", "relation", "tail_type", "tail"])

		for i, line in enumerate(fin):
			line = line.rstrip("\n")
			if not line:
				continue
			parts = line.split("\t")
			if len(parts) != 3:
				continue
			head, relation, tail = parts
			h_type, h_id = split_prefix(head)
			t_type, t_id = split_prefix(tail)
			writer.writerow([h_type, h_id, relation, t_type, t_id])

			if max_rows is not None and (i + 1) >= max_rows:
				break

def main():
	if not os.path.exists(TRIPLES_TXT):
		raise FileNotFoundError(f"未找到 {TRIPLES_TXT}")

	# 生成样本（10k）
	convert_file(TRIPLES_TXT, TRIPLES_TSV_SAMPLE, max_rows=10_000)
	print(f"写出样本：{TRIPLES_TSV_SAMPLE}")

	# 生成全量 TSV
	convert_file(TRIPLES_TXT, TRIPLES_TSV, max_rows=None)
	print(f"写出全量：{TRIPLES_TSV}")

if __name__ == "__main__":
	main()

# http://219.228.60.32:7474/browser/
# username: neo4j
# password: b230b230
#   cypher-shell -a neo4j://localhost:7687 -u neo4j -p b230b230 --database neo4j "

