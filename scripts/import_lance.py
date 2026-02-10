import os
import csv
import pandas as pd
import pyarrow as pa
import lancedb
from tqdm import tqdm

"""
将 TSV 文件导入 LanceDB 数据库。
支持流式读取大文件，避免内存溢出。
"""

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")
TRIPLES_TSV_SAMPLE = os.path.join(DATA_DIR, "triples_sample_10k.tsv")

# LanceDB 数据库路径
LANCEDB_PATH = os.path.join(PROJECT_ROOT, "storage", "lance")
TABLE_NAME_FULL = "triples"
TABLE_NAME_SAMPLE = "triples_sample_10k"


def import_tsv_to_lancedb(tsv_path: str, db_path: str, table_name: str, batch_size: int = 100_000):
    """
    流式读取 TSV 文件，分批写入 LanceDB。
    
    Args:
        tsv_path: TSV 文件路径
        db_path: LanceDB 数据库路径
        table_name: 表名
        batch_size: 每批处理的行数
    """
    if not os.path.exists(tsv_path):
        raise FileNotFoundError(f"未找到文件: {tsv_path}")
    
    # 先统计总行数（用于进度条）
    total_rows = 0
    with open(tsv_path, "r", encoding="utf-8") as f:
        # 跳过表头
        next(f)
        total_rows = sum(1 for _ in f)
    
    print(f"📊 总行数: {total_rows:,}")
    
    # 连接到 LanceDB
    db = lancedb.connect(db_path)
    
    # 流式读取并分批写入
    batch_data = []
    first_batch = True
    
    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        
        for row in tqdm(reader, total=total_rows, desc="读取并写入 TSV"):
            batch_data.append({
                "head_type": row["head_type"],
                "head": row["head"],
                "relation": row["relation"],
                "tail_type": row["tail_type"],
                "tail": row["tail"],
            })
            
            # 达到批次大小时，写入 LanceDB
            if len(batch_data) >= batch_size:
                df = pd.DataFrame(batch_data)
                
                if first_batch:
                    # 第一批：创建表（覆盖模式）
                    tbl = db.create_table(table_name, data=df, mode="overwrite")
                    first_batch = False
                else:
                    # 后续批次：追加数据
                    tbl.add(df)
                
                batch_data = []
        
        # 处理最后一批
        if batch_data:
            df = pd.DataFrame(batch_data)
            if first_batch:
                tbl = db.create_table(table_name, data=df, mode="overwrite")
            else:
                tbl.add(df)
    
    # 验证写入结果
    if not first_batch or batch_data:
        print(f"📝 写入完成...")
        print(f"✅ 已写入表: {table_name}")
        print(f"   总行数: {tbl.count_rows():,}")
        print(f"   列: {', '.join(tbl.schema.names)}")
    else:
        print("⚠️  没有数据可写入")


def verify_lancedb_table(db_path: str, table_name: str, num_rows: int = 5):
    """验证 LanceDB 表，显示前几行"""
    if not os.path.exists(db_path):
        print(f"❌ 数据库不存在: {db_path}")
        return
    
    db = lancedb.connect(db_path)
    
    if table_name not in db.table_names():
        print(f"❌ 表不存在: {table_name}")
        return
    
    tbl = db[table_name]
    print(f"\n📖 验证表: {table_name}")
    print(f"   总行数: {tbl.count_rows():,}")
    print(f"   列: {', '.join(tbl.schema.names)}")
    
    # 显示前几行
    df = tbl.head(num_rows).to_pandas()
    print(f"\n前 {num_rows} 行数据:")
    print(df.to_string(index=False))

def verify_take_offset_usage(db_path: str, table_name: str):
    """
    展示 take_offsets 和 take_row_ids 的基本使用。
    """
    import time
    
    db = lancedb.connect(db_path)
    tbl = db[table_name]
    total_rows = tbl.count_rows()
    
    print("=" * 70)
    print("🔬 take_offsets 和 take_row_ids 基本使用")
    print("=" * 70)
    print(f"表名: {table_name}, 总行数: {total_rows:,}")
    
    # ========== 1. take_offsets 基本使用 ==========
    print("\n📊 1. take_offsets 基本使用")
    print("-" * 70)
    print("用法: tbl.take_offsets(offsets_list).to_pandas()")
    print("参数: offsets 是从 0 开始的连续索引，范围 [0, count_rows())")
    print()
    
    test_offsets = [0, 1, 2, 5, 10]
    start = time.time()
    result_offsets = tbl.take_offsets(test_offsets).to_pandas()
    elapsed = (time.time() - start) * 1000
    
    print(f"示例: tbl.take_offsets({test_offsets})")
    print(f"耗时: {elapsed:.3f} ms, 返回 {len(result_offsets)} 行")
    print(result_offsets[["head", "relation", "tail"]].to_string(index=False))
    
    # ========== 2. take_row_ids 基本使用 ==========
    print("\n📊 2. take_row_ids 基本使用")
    print("-" * 70)
    print("用法: tbl.take_row_ids(row_ids_list).to_pandas()")
    print("参数: row_ids 需要先通过 search().with_row_id(True) 获取")
    print()
    
    # 先获取 row_ids
    search_result = tbl.search().with_row_id(True).limit(5).to_pandas()
    row_ids = search_result["_rowid"].astype(int).tolist()
    
    start = time.time()
    result_row_ids = tbl.take_row_ids(row_ids).to_pandas()
    elapsed = (time.time() - start) * 1000
    
    print(f"获取的 row_ids: {row_ids}")
    print(f"示例: tbl.take_row_ids({row_ids})")
    print(f"耗时: {elapsed:.3f} ms, 返回 {len(result_row_ids)} 行")
    print(result_row_ids[["head", "relation", "tail"]].to_string(index=False))
    
    print("\n" + "=" * 70)


def benchmark_query_methods(db_path: str, table_name: str, num_samples: int = 10, batch_size: int = 100):
    """
    对比三种查询方式的性能：where 查询 vs take_row_ids vs take_offsets
    
    测试思路：
    1. 预先获取测试样本的 head 节点及其对应的 row_ids/offsets
    2. 分别用三种方式查询，记录耗时
    3. 对比性能差异
    """
    import time
    import statistics
    import random
    
    db = lancedb.connect(db_path)
    tbl = db[table_name]
    total_rows = tbl.count_rows()
    
    print("=" * 70)
    print("🚀 查询方式性能对比: where vs take_row_ids vs take_offsets")
    print("=" * 70)
    
    # ==================== 准备阶段 ====================
    print("\n📋 准备阶段: 获取测试样本")
    print("-" * 70)
    
    # 随机选取若干行（使用 take_offsets 获取真正的随机样本）
    random_offsets = random.sample(range(total_rows), num_samples)
    sample_data = tbl.take_offsets(random_offsets).to_pandas()
    

    # 提取测试样本: [(head, row_id, offset), ...]
    # 注意: 当前表中 row_id == offset（无删除/压缩）
    test_samples = []
    for i, offset in enumerate(random_offsets):
        test_samples.append({
            "head": sample_data.iloc[i]["head"],
            "row_id": offset,  # 当前 row_id == offset
            "offset": offset
        })

    print(f"测试样本数: {num_samples}")
    print(f"批量大小: {batch_size}")
    print(f"样本示例: head='{test_samples[0]['head'][:40]}...', row_id={test_samples[0]['row_id']}")
    
    # ==================== 单次查询测试 ====================
    print("\n📊 测试 1: 单次查询性能")
    print("-" * 70)
    
    times_where = []
    times_row_ids = []
    times_offsets = []
    
    # 预热查询（消除冷启动影响）
    _ = tbl.search().where(f"head = '{test_samples[0]['head']}'").limit(1).to_pandas()
    _ = tbl.take_row_ids([test_samples[0]["row_id"]]).to_pandas()
    _ = tbl.take_offsets([test_samples[0]["offset"]]).to_pandas()
    
    for sample in test_samples:
         # A. where 查询
        start = time.time()
        _ = tbl.search().where(f"head = '{sample['head']}'").limit(1).to_pandas()
        times_where.append((time.time() - start) * 1000)
        # B. take_row_ids
        start = time.time()
        _ = tbl.take_row_ids([sample["row_id"]]).to_pandas()
        times_row_ids.append((time.time() - start) * 1000)
        
        # C. take_offsets
        start = time.time()
        _ = tbl.take_offsets([sample["offset"]]).to_pandas()
        times_offsets.append((time.time() - start) * 1000)


    
    # 打印单次查询结果
    print(f"{'方法':<20} {'平均(ms)':<12} {'中位数(ms)':<12} {'最小(ms)':<12} {'最大(ms)':<12}")
    print("-" * 68)
    print(f"{'where 查询':<20} {statistics.mean(times_where):<12.3f} {statistics.median(times_where):<12.3f} {min(times_where):<12.3f} {max(times_where):<12.3f}")
    print(f"{'take_row_ids':<20} {statistics.mean(times_row_ids):<12.3f} {statistics.median(times_row_ids):<12.3f} {min(times_row_ids):<12.3f} {max(times_row_ids):<12.3f}")
    print(f"{'take_offsets':<20} {statistics.mean(times_offsets):<12.3f} {statistics.median(times_offsets):<12.3f} {min(times_offsets):<12.3f} {max(times_offsets):<12.3f}")
    
    # 计算加速比
    speedup_row_ids = statistics.mean(times_where) / statistics.mean(times_row_ids)
    speedup_offsets = statistics.mean(times_where) / statistics.mean(times_offsets)
    print(f"\n加速比: take_row_ids 比 where 快 {speedup_row_ids:.1f}x, take_offsets 比 where 快 {speedup_offsets:.1f}x")
    
    # ==================== 批量查询测试 ====================
    print(f"\n📊 测试 2: 批量查询性能 (batch_size={batch_size})")
    print("-" * 70)
    
    num_batch_tests = 10
    batch_times_where = []
    batch_times_row_ids = []
    batch_times_offsets = []
    
    # 预热
    _ = tbl.take_row_ids([0, 1, 2]).to_pandas()
    
    for _ in range(num_batch_tests):
        # 每次测试使用不同的随机数据
        batch_offsets = random.sample(range(total_rows), batch_size)
        batch_data = tbl.take_offsets(batch_offsets).to_pandas()
        batch_heads = batch_data["head"].tolist()
        batch_row_ids = batch_offsets  # 当前 row_id == offset
        
        # A. where 批量查询 (使用 OR 条件，只取10个避免条件过长)
        heads_sample = random.sample(batch_heads, 10)
        condition = " OR ".join([f"head = '{h}'" for h in heads_sample])
        start = time.time()
        _ = tbl.search().where(condition).limit(100).to_pandas()
        batch_times_where.append((time.time() - start) * 1000)
        
        # B. take_row_ids 批量
        start = time.time()
        _ = tbl.take_row_ids(batch_row_ids).to_pandas()
        batch_times_row_ids.append((time.time() - start) * 1000)
        
        # C. take_offsets 批量
        start = time.time()
        _ = tbl.take_offsets(batch_offsets).to_pandas()
        batch_times_offsets.append((time.time() - start) * 1000)
    
    # 打印批量查询结果
    print(f"{'方法':<25} {'平均(ms)':<12} {'查询条数':<12} {'每条耗时(ms)':<15}")
    print("-" * 64)
    avg_where = statistics.mean(batch_times_where)
    avg_row_ids = statistics.mean(batch_times_row_ids)
    avg_offsets = statistics.mean(batch_times_offsets)
    per_item_where = avg_where / 10
    per_item_row_ids = avg_row_ids / batch_size
    per_item_offsets = avg_offsets / batch_size
    print(f"{'where (10条OR)':<25} {avg_where:<12.3f} {10:<12} {per_item_where:<15.3f}")
    print(f"{'take_row_ids':<25} {avg_row_ids:<12.3f} {batch_size:<12} {per_item_row_ids:<15.3f}")
    print(f"{'take_offsets':<25} {avg_offsets:<12.3f} {batch_size:<12} {per_item_offsets:<15.3f}")
    
    # 计算批量查询加速比（按每条耗时计算）
    batch_speedup_row_ids = per_item_where / per_item_row_ids
    batch_speedup_offsets = per_item_where / per_item_offsets
    print(f"\n加速比: take_row_ids 比 where 快 {batch_speedup_row_ids:.1f}x, take_offsets 比 where 快 {batch_speedup_offsets:.1f}x")
    
    # ==================== 结论 ====================
    print("\n📝 结论")
    print("-" * 70)
    print(f"• 单次查询: take_row_ids 比 where 快 {speedup_row_ids:.1f}x")
    print(f"• 批量查询: take_row_ids 比 where 快 {batch_speedup_row_ids:.1f}x (每条仅需 {per_item_row_ids:.3f}ms)")
    print(f"• 对于方案二邻接表: 预存 row_id 后查询可获得显著性能提升")
    
    print("\n" + "=" * 70)


def main():
    """主函数"""
    print("=" * 60)
    print("将 TSV 文件导入 LanceDB 数据库")
    print(f"数据库路径: {LANCEDB_PATH}")
    print("=" * 60)
    
    # 确保数据库目录存在
    os.makedirs(LANCEDB_PATH, exist_ok=True)
    
    # 导入样本文件
    if os.path.exists(TRIPLES_TSV_SAMPLE):
        print(f"\n1️⃣  导入样本文件: {TRIPLES_TSV_SAMPLE}")
        import_tsv_to_lancedb(TRIPLES_TSV_SAMPLE, LANCEDB_PATH, TABLE_NAME_SAMPLE, batch_size=10_000)
        verify_lancedb_table(LANCEDB_PATH, TABLE_NAME_SAMPLE)
    
    # 导入全量文件
    # if os.path.exists(TRIPLES_TSV):
    #     file_size = os.path.getsize(TRIPLES_TSV)
    #     if file_size > 0:
    #         print(f"\n2️⃣  导入全量文件: {TRIPLES_TSV}")
    #         print(f"   文件大小: {file_size / 1024 / 1024:.2f} MB")
    #         import_tsv_to_lancedb(TRIPLES_TSV, LANCEDB_PATH, TABLE_NAME_FULL, batch_size=100_000)
    #         verify_lancedb_table(LANCEDB_PATH, TABLE_NAME_FULL, num_rows=5)
    #     else:
    #         print(f"\n⚠️  全量文件为空，跳过: {TRIPLES_TSV}")
    # else:
    #     print(f"\n⚠️  全量文件不存在，跳过: {TRIPLES_TSV}")
    
    # print("\n" + "=" * 60)
    # print("✅ 导入完成！")
    # print(f"数据库位置: {LANCEDB_PATH}")
    # print("=" * 60)
    
    # 验证 take_offsets 和 take_row_ids 的基本用法
    # verify_take_offset_usage(LANCEDB_PATH, TABLE_NAME_FULL)
    
    # 性能对比测试
    # print("\n")
    # benchmark_query_methods(LANCEDB_PATH, TABLE_NAME_FULL, num_samples=100, batch_size=100)


if __name__ == "__main__":
    main()

