# 设置私钥
set COLONY_PRIVATE_KEY=你的base58私钥

# 查看当前汇率
python colony_onchain.py rates

# 自动发现用户 PDA（首次需一次游戏内 swap 记录）
python3 colony_onchain.py discover

# 启动监控
python3 colony_onchain.py monitor          # dry-run
python3 colony_onchain.py monitor --live   # 实际交易
