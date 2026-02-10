# 下载 huggingkg/tiny-kg 到本地任意目录
# huggingface-cli download \
#   --repo-type dataset \
#   --resume-download \
#   cqsss/HuggingKG \
#   --local-dir ./data/huggingkg_tiny \
#   --local-dir-use-symlinks False

from datasets import load_from_disk, load_dataset
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
data_dir = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")

# 如果是 datasets 的 save_to_disk 目录结构，直接加载；否则回退到从 triples.txt 构建
dataset_dict_path = os.path.join(data_dir, "dataset_dict.json")
triples_path = os.path.join(data_dir, "triples.txt")

if os.path.exists(dataset_dict_path):
	ds = load_from_disk(data_dir)  # 返回 DatasetDict
else:
	if not os.path.exists(triples_path):
		raise FileNotFoundError(
			f"未找到可加载的数据：既没有 {dataset_dict_path} 也没有 {triples_path}"
		)
	# triples.txt 为制表符分隔的三元组：head\trelation\ttail
	ds = load_dataset(
		"csv",
		data_files=triples_path,
		delimiter="\t",
		column_names=["head", "relation", "tail"],
	)

print(ds)               # 查看 split、字段
print(ds["train"][0])   # 查看第一条三元组
