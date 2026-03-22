# Lance `_rowaddr` 测试结论

本文档记录 `test_lance_api/test_rowaddr.py` 的实验结论，用于说明 Lance / LanceDB 中 `_rowaddr` 的含义与当前观察到的行为。

## 结论摘要

在当前测试环境与当前版本下，可以将 `_rowaddr` 理解为一个 `u64` 编码值：

$$
\text{rowaddr} = (\text{fragment\_id} \ll 32) + \text{offset}
$$

即：

- 高 32 位表示 `fragment_id`
- 低 32 位表示该行在 fragment 内的 `offset`

解码方式为：

$$
\text{fragment\_id} = \text{rowaddr} \gg 32
$$

$$
\text{offset} = \text{rowaddr} \& 0xFFFFFFFF
$$

## 当前测试观察

`test_rowaddr.py` 中的实验表明，在当前版本和这些测试场景下：

- `_rowaddr` 可以通过 `LanceDataset.to_table(..., with_row_address=True)` 读出
- `_rowid` 与 `_rowaddr` 数值相同
- 但更本质的信息是：`_rowaddr` 的值编码了 `fragment_id` 与 `offset`

也就是说，当前实验中可以观察到：

$$
\_rowid = \_rowaddr
$$

但这属于当前实现与当前测试场景下的现象；更稳定的结论是 `_rowaddr` 的位编码结构。

## 实验 1：单次写入

单批次插入 5 行时，输出类似：

- `_rowaddr`: `[0, 1, 2, 3, 4]`
- 解码后：`[(0, 0), (0, 1), (0, 2), (0, 3), (0, 4)]`

说明所有行都在：

- `fragment_id = 0`
- `offset = 0..4`

## 实验 2：两次 append

分两批写入后，输出类似：

- 第一批：`0, 1, 2`
- 第二批：`4294967296, 4294967297, 4294967298`

其中：

- `4294967296 = 2^{32}`
- `4294967297 = 2^{32} + 1`
- `4294967298 = 2^{32} + 2`

解码后对应：

- 第一批：`(0, 0), (0, 1), (0, 2)`
- 第二批：`(1, 0), (1, 1), (1, 2)`

说明第二次 append 写入了新的 fragment：

- 第一批位于 `fragment_id = 0`
- 第二批位于 `fragment_id = 1`

## 实验 3：delete 后再 optimize

删除部分行后，仍能观察到保留行维持原有 fragment / offset 编码，例如：

- `0 -> (0, 0)`
- `2 -> (0, 2)`
- `4294967296 -> (1, 0)`
- `4294967298 -> (1, 2)`

这说明 delete 不会立即把剩余行重排为新的连续 offset。

执行 `optimize()` 后，数据被重写到新 fragment 中，输出类似：

- `8589934592, 8589934593, 8589934594, 8589934595`

其中：

- `8589934592 = 2^{33}`

解码后对应：

- `(2, 0), (2, 1), (2, 2), (2, 3)`

说明 optimize 后：

- 数据被写入新的 `fragment_id = 2`
- `offset` 重新从 `0` 开始连续编号

## 实践意义

从测试看，`_rowaddr` 更适合作为：

- 行在 Lance 底层 fragment 中的位置标识
- 用于理解数据布局、fragment 分布、append / optimize 后的物理变化

而不能简单把它理解为“全局连续行号”。

## 对当前项目的直接结论

在本项目当前测试中，可以采用如下表述：

1. `_rowaddr` 是一个 `u64`
2. 它的高 32 位是 `fragment_id`
3. 它的低 32 位是该行在 fragment 内的 `offset`
4. 在当前版本与当前测试场景下，观测到 `_rowid == _rowaddr`
5. append / delete / optimize 会改变或保留其编码方式，具体取决于是否发生 fragment 重写

## 对应测试文件

相关实验代码见：

- `test_lance_api/test_rowaddr.py`

可用如下命令复现：

- `uv run pytest test_lance_api/test_rowaddr.py -v -s`
- `pytest test_lance_api/test_rowaddr.py -v -s`

## 注意

上述结论来自当前版本的实验观察，适用于当前测试环境。若 Lance / LanceDB 内部实现发生变化，应重新运行测试确认行为是否一致。
