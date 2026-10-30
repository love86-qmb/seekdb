# AMD 赛题提交说明（seekdb × hipVS）

> 赛题：使用 hipVS 加速 HNSW 索引构建（CAGRA 建图链路优化）
> 本文件只做提交说明，**不改变任何源码行为**。

## 1. 这次提交是什么

- **代码内容 = 官方冻结基线 `amd-cagra-contest-baseline-v5`**（commit `634888d9b037153fa143d2c913ba4b25503e1d62`）**原样**，
  本提交在其之上只新增本文档，目的是让答题分支成为冻结基线的**合法后代**（评测要求 commit 必须基于 V5 冻结基线开发）。
- 因此本轮**没有任何性能改动**，预期结果是：正确性与硬门槛通过 → **4 分**（题面第 5 节：直接提交未修改的基线只能获得 4 分），
  CAGRA 构建加速比 ≈ 1.0x、查询加速比 ≈ 1.0x → 性能分 0 分。

## 2. 为什么先这样提

这次提交的首要目的是**打通评测链路**，一次性验证三个此前只能盲猜的环节：

1. 报名登记的答题仓库地址是否满足 `https://<用户名>:<令牌>@github.com/<所有者>/<仓库>` 格式要求（不满足直接 failMsg = 4）；
2. 固定编译命令（`OB_BUILD_CUVS=ON`、`CUVS_BRIDGE_LIB`）在本仓库能否编译通过（失败 failMsg = 3）；
3. 外部 profiler 的 GPU CAGRA 路径抽检能否通过（不通过则 GPU 阶段不进入正式测试）。

三项都过之后，后续任何真实优化才有被计分的可能。

## 3. 后续优化方向（尚未实施）

围绕题面第 3 节允许的范围：

- 建图算法与批处理流程：调整 CAGRA 的 `graph_degree` / `intermediate_graph_degree` / `itopk_size` 等构建参数，
  在满足 Recall@10 / Recall@100 硬门槛的前提下压缩构建时间；
- CPU↔GPU 数据传输：减少建图前的重复拷贝与不必要的同步点；
- 显存分配与索引生命周期：复用中间缓冲，避免构建期反复申请释放；
- 索引持久化 / 重载：缩短「构建完成 → 可查询」这段，注意第一条查询的惰性建图必须计入构建时间。

以上改动都必须在 **AMD Radeon GPU + ROCm 7.2.4** 环境实测对比（与同轮冻结基线配对比较），
没有本地 GPU 实测数据的改动不会提交，避免因 Recall 门槛或 GPU 路径抽检不通过而导致 0 分。

## 4. 复现编译（按题面统一流程）

```bash
cd tools/hipvs_bridge && bash build.sh          # 产出 /libseekdb_cuvs_bridge.so
cmake -S . -B build_release -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DOB_ENABLE_UNITY=ON \
  -DOB_USE_LLD=ON \
  -DOB_BUILD_CUVS=ON \
  -DOB_BUILD_CUVS_TRACE=OFF \
  -DCUVS_BRIDGE_LIB=/libseekdb_cuvs_bridge.so
cmake --build build_release --target seekdb
```

## 5. 备注

- 本仓库保持与官方基线一致，未修改 `deps/3rd` 与 Rust 依赖；
- 未引入任何需要联网下载的依赖；
- 未使用预编译产物，评测从源码构建。
