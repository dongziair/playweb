"""
PlayColony 链上自动套利脚本

完全脱离浏览器，通过私钥直接与 Solana 链上合约交互。

使用步骤：
1. pip install solana solders
2. 编辑 .env 文件，填入你的 base58 私钥
3. python colony_onchain.py discover   # 自动发现用户 PDA 地址
4. python colony_onchain.py rates      # 查看当前汇率
5. python colony_onchain.py monitor    # 持续监控（dry-run）
6. python colony_onchain.py monitor --live  # 实际交易
"""

import json
import struct
import time
import sys
import os
import logging
from datetime import datetime, timedelta
from itertools import permutations
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.instruction import Instruction, AccountMeta
from solders.transaction import Transaction
from solders.message import Message
from solders.hash import Hash

# ============================================================
# 常量
# ============================================================

# Ephemeral Rollup RPC（游戏实际运行在这里，不是 mainnet）
RPC_URL = "https://as.magicblock.app/"
RESOURCE_PROGRAM = Pubkey.from_string("2K2374VEqxbFJWycxoj8ub2wBk7KwwnNn7M5V7QsL9r2")
POOL_STATE = Pubkey.from_string("AdQJrDXwWAeBPc254qnLBCWfyTqJqoAahRgZ4kok3PZD")

# swap 指令 discriminator（从用户实际交易逆向得到）
SWAP_DISCRIMINATOR = bytes.fromhex("8dac0ad04509389a")
# collect/claim 指令 discriminator（swap 前的预处理指令）
COLLECT_DISCRIMINATOR = bytes.fromhex("49047707f2ff1de2")

# 资源编号
METAL = 0
GAS = 1
CRYSTAL = 2
RESOURCE_NAMES = {0: "Metal", 1: "Gas", 2: "Crystal"}

# 交易参数
FEE = 0.003
MIN_PROFIT = 0.003
CHECK_INTERVAL = 5
MAX_TRADES_PER_HOUR = 30

# 文件路径
BASE_DIR = Path(__file__).parent
ENV_FILE = BASE_DIR / ".env"
PDA_FILE = BASE_DIR / "user_pdas.json"
LOG_FILE = Path(__file__).parent / "onchain_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("colony_onchain")


B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s: str) -> bytes:
    n = 0
    for c in s:
        n = n * 58 + B58_ALPHABET.index(c)
    if n == 0:
        return b"\x00"
    return n.to_bytes((n.bit_length() + 7) // 8, "big")


# ============================================================
# .env 加载 + 密钥加载
# ============================================================

def load_env():
    """从 .env 文件加载配置到环境变量"""
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                key, val = key.strip(), val.strip()
                if val and val != "在这里填入你的base58私钥":
                    os.environ.setdefault(key, val)


def load_keypair() -> Keypair:
    """从 .env 文件或环境变量加载钱包密钥"""
    load_env()

    pk = os.environ.get("COLONY_PRIVATE_KEY")
    if pk:
        try:
            return Keypair.from_base58_string(pk)
        except ValueError:
            raw = b58decode(pk)
            log.info(f"key 字符数: {len(pk)}, 解码字节数: {len(raw)}")
            if len(raw) >= 64:
                return Keypair.from_bytes(raw[:64])
            return Keypair.from_bytes(raw)

    path = os.environ.get("COLONY_KEYPAIR_PATH")
    if path and Path(path).exists():
        data = json.loads(Path(path).read_text())
        return Keypair.from_bytes(bytes(data))

    print("请在 .env 文件中填入私钥:")
    print(f"  文件路径: {ENV_FILE}")
    print("  格式: COLONY_PRIVATE_KEY=你的base58私钥")
    sys.exit(1)


# ============================================================
# 链上数据读取
# ============================================================

class PoolReader:
    """从链上读取资源池数据并计算汇率"""

    def __init__(self, rpc: Client):
        self.rpc = rpc

    def read_pool_data(self) -> bytes:
        acct = self.rpc.get_account_info(POOL_STATE, commitment=Confirmed)
        if not acct.value:
            raise RuntimeError("无法读取资源池账户")
        return bytes(acct.value.data)

    def get_rates(self) -> Dict[str, float]:
        """读取并解析三组汇率

        数据结构 (120 bytes):
        [0:8]   Borsh discriminator
        [8:40]  Entity Pubkey (32 bytes)
        [40:42] Header (u16, 用途未知)
        [42:120] 3 组 TradingPool, 每组 26 bytes:
            u8  resource_a
            u8  resource_b
            u64 reserve_a
            u64 reserve_b
            u64 累计交易量或常数 k
        """
        data = self.read_pool_data()

        rates = {}
        for pool_idx in range(3):
            base = 42 + pool_idx * 26
            res_a = data[base]
            res_b = data[base + 1]
            reserve_a = struct.unpack("<Q", data[base + 2 : base + 10])[0]
            reserve_b = struct.unpack("<Q", data[base + 10 : base + 18])[0]

            if reserve_a == 0 or reserve_b == 0:
                continue

            rate = reserve_b / reserve_a
            name_a = RESOURCE_NAMES.get(res_a, str(res_a))
            name_b = RESOURCE_NAMES.get(res_b, str(res_b))
            rates[f"{name_a}_{name_b}"] = rate

        return rates


# ============================================================
# PDA 发现
# ============================================================

class PDADiscovery:
    """从用户历史交易中发现 PDA 地址"""

    def __init__(self, rpc: Client, user: Pubkey):
        self.rpc = rpc
        self.user = user

    def discover(self) -> dict:
        """多种策略搜索用户 PDA"""
        log.info(f"钱包地址: {self.user}")

        # 策略1: 从 pool 账户的交易中筛选
        pdas = self._search_pool_txs()
        if pdas:
            return pdas

        # 策略2: 从用户钱包的所有交易中搜索
        pdas = self._search_user_txs()
        if pdas:
            return pdas

        log.warning("未找到 swap 交易记录")
        log.warning("请先在游戏中手动执行一次 swap，然后重新运行 discover")
        return {}

    def _search_pool_txs(self) -> Optional[dict]:
        """从 pool 账户的交易中筛选当前用户的 swap"""
        log.info("搜索 pool 账户交易记录...")
        sigs = self.rpc.get_signatures_for_address(POOL_STATE, limit=200)
        user_str = str(self.user)

        for sig_info in sigs.value:
            pdas = self._check_tx(sig_info.signature, user_str)
            if pdas:
                return pdas
        return None

    def _search_user_txs(self) -> Optional[dict]:
        """从用户钱包的所有交易中搜索"""
        log.info("搜索用户钱包交易记录...")
        sigs = self.rpc.get_signatures_for_address(self.user, limit=200)
        user_str = str(self.user)

        for sig_info in sigs.value:
            pdas = self._check_tx(sig_info.signature, user_str)
            if pdas:
                return pdas

            # 也检查非 swap 但与 ResourceProgram 交互的交易
            tx = self.rpc.get_transaction(
                sig_info.signature, max_supported_transaction_version=0
            )
            if not tx.value:
                continue
            msg = tx.value.transaction.transaction.message
            acct_keys = [str(k) for k in msg.account_keys]
            for ix in msg.instructions:
                prog = acct_keys[ix.program_id_index]
                if prog == str(RESOURCE_PROGRAM):
                    raw = b58decode(ix.data)
                    ix_accounts = [acct_keys[idx] for idx in ix.accounts]
                    log.info(f"发现 ResourceProgram 交互: disc={raw[:8].hex()}")
                    log.info(f"  accounts: {ix_accounts}")

        return None

    def _check_tx(self, signature, user_str: str) -> Optional[dict]:
        """检查单笔交易中是否包含 swap 指令"""
        tx = self.rpc.get_transaction(
            signature, max_supported_transaction_version=0
        )
        if not tx.value:
            return None

        msg = tx.value.transaction.transaction.message
        acct_keys = [str(k) for k in msg.account_keys]

        if user_str not in acct_keys:
            return None

        for ix in msg.instructions:
            raw = b58decode(ix.data)
            if raw[:8] == SWAP_DISCRIMINATOR:
                ix_accounts = [acct_keys[idx] for idx in ix.accounts]
                pdas = {
                    "signer": ix_accounts[0],
                    "player_entity": ix_accounts[1],
                    "player_component": ix_accounts[2],
                    "pool_state": ix_accounts[3],
                    "player_data": ix_accounts[4],
                    "user_state": ix_accounts[5],
                }
                log.info("找到 PDA:")
                for name, addr in pdas.items():
                    log.info(f"  {name}: {addr}")
                return pdas
        return None


# ============================================================
# 套利引擎
# ============================================================

class ArbitrageEngine:
    def __init__(self, fee=FEE, min_profit=MIN_PROFIT):
        self.fee_mul = 1 - fee
        self.min_profit = min_profit

    def find_opportunities(self, rates: Dict[str, float]) -> List[dict]:
        resources = ["Metal", "Gas", "Crystal"]
        full = {}
        for k, v in rates.items():
            if v is None or v <= 0:
                continue
            sell, buy = k.split("_")
            full[(sell, buy)] = v
            full[(buy, sell)] = 1.0 / v

        opps = []
        for perm in permutations(resources):
            a, b, c = perm
            r1 = full.get((a, b))
            r2 = full.get((b, c))
            r3 = full.get((c, a))
            if None in (r1, r2, r3):
                continue
            gross = r1 * r2 * r3
            net = gross * (self.fee_mul**3)
            opps.append(
                {
                    "path": [a, b, c, a],
                    "rates": [r1, r2, r3],
                    "gross": gross,
                    "net": net,
                    "profit": net - 1.0,
                }
            )
        opps.sort(key=lambda x: x["profit"], reverse=True)
        return opps


# ============================================================
# 交易构建 & 发送
# ============================================================

class SwapExecutor:
    RES_MAP = {"Metal": 0, "Gas": 1, "Crystal": 2}

    def __init__(self, rpc: Client, keypair: Keypair, pdas: dict, dry_run=True):
        self.rpc = rpc
        self.keypair = keypair
        self.pdas = pdas
        self.dry_run = dry_run
        self.trade_count = 0
        self.hour_start = datetime.now()

    def can_trade(self) -> bool:
        now = datetime.now()
        if now - self.hour_start > timedelta(hours=1):
            self.trade_count = 0
            self.hour_start = now
        return self.trade_count < MAX_TRADES_PER_HOUR

    def execute_swap(self, sell: str, buy: str, amount: int = 1000) -> Optional[str]:
        """构建并发送 swap 交易，返回签名"""
        if not self.can_trade():
            log.warning("每小时交易上限")
            return None

        sell_type = self.RES_MAP[sell]
        buy_type = self.RES_MAP[buy]

        # 指令1: collect（预处理）
        collect_data = COLLECT_DISCRIMINATOR
        collect_accounts = [
            AccountMeta(self.keypair.pubkey(), is_signer=True, is_writable=True),
            AccountMeta(Pubkey.from_string(self.pdas["player_entity"]), is_signer=False, is_writable=False),
            AccountMeta(Pubkey.from_string(self.pdas["player_component"]), is_signer=False, is_writable=True),
            AccountMeta(Pubkey.from_string(self.pdas["player_data"]), is_signer=False, is_writable=True),
            AccountMeta(Pubkey.from_string(self.pdas["user_state"]), is_signer=False, is_writable=True),
        ]
        ix_collect = Instruction(RESOURCE_PROGRAM, collect_data, collect_accounts)

        # 指令2: swap
        # args = u8(sell) + u8(buy) + u64(amount) + u64(min_amount_out)
        swap_data = SWAP_DISCRIMINATOR + struct.pack('<BB', sell_type, buy_type) + struct.pack('<Q', amount) + struct.pack('<Q', 0)
        swap_accounts = [
            AccountMeta(self.keypair.pubkey(), is_signer=True, is_writable=True),
            AccountMeta(Pubkey.from_string(self.pdas["player_entity"]), is_signer=False, is_writable=False),
            AccountMeta(Pubkey.from_string(self.pdas["player_component"]), is_signer=False, is_writable=True),
            AccountMeta(POOL_STATE, is_signer=False, is_writable=True),
            AccountMeta(Pubkey.from_string(self.pdas["player_data"]), is_signer=False, is_writable=True),
            AccountMeta(Pubkey.from_string(self.pdas["user_state"]), is_signer=False, is_writable=True),
        ]
        ix_swap = Instruction(RESOURCE_PROGRAM, swap_data, swap_accounts)

        instructions = [ix_collect, ix_swap]

        if self.dry_run:
            log.info(f"  [DRY-RUN] {sell} → {buy}, amount={amount}")
            try:
                blockhash = self.rpc.get_latest_blockhash(Confirmed).value.blockhash
                msg = Message.new_with_blockhash(instructions, self.keypair.pubkey(), blockhash)
                tx = Transaction.new_unsigned(msg)
                tx.sign([self.keypair], blockhash)
                sim = self.rpc.simulate_transaction(tx)
                if sim.value.err:
                    log.warning(f"  模拟失败: {sim.value.err}")
                else:
                    log.info(f"  模拟成功 ✓")
                    for line in (sim.value.logs or []):
                        if "swap" in line.lower() or "trade" in line.lower() or "error" in line.lower():
                            log.info(f"    {line}")
            except Exception as e:
                log.warning(f"  模拟异常: {e}")
            return None

        try:
            blockhash = self.rpc.get_latest_blockhash(Confirmed).value.blockhash
            msg = Message.new_with_blockhash(instructions, self.keypair.pubkey(), blockhash)
            tx = Transaction.new_unsigned(msg)
            tx.sign([self.keypair], blockhash)
            result = self.rpc.send_transaction(tx)
            sig = str(result.value)
            log.info(f"  交易已发送: {sig}")
            self.trade_count += 1
            return sig
        except Exception as e:
            log.error(f"  发送失败: {e}")
            return None

    def execute_path(self, path: List[str]) -> bool:
        """执行三角套利路径"""
        for i in range(len(path) - 1):
            sell, buy = path[i], path[i + 1]
            log.info(f"  [{i+1}/{len(path)-1}] {sell} → {buy}")
            result = self.execute_swap(sell, buy)
            if not self.dry_run and result is None:
                return False
            if not self.dry_run:
                time.sleep(2)
        return True


# ============================================================
# 主循环
# ============================================================

class Bot:
    def __init__(self, dry_run=True):
        self.keypair = load_keypair()
        self.rpc = Client(RPC_URL)

        # 加载用户 PDA
        if not PDA_FILE.exists():
            log.error("请先运行: python colony_onchain.py discover")
            sys.exit(1)
        self.pdas = json.loads(PDA_FILE.read_text())

        self.pool_reader = PoolReader(self.rpc)
        self.engine = ArbitrageEngine()
        self.executor = SwapExecutor(self.rpc, self.keypair, self.pdas, dry_run)
        self.dry_run = dry_run
        self.cycle = 0

    def run(self):
        mode = "DRY-RUN" if self.dry_run else "⚡ LIVE"
        log.info(f"启动链上套利监控 [{mode}]")
        log.info(f"钱包: {self.keypair.pubkey()}")
        log.info(f"阈值: {MIN_PROFIT*100:.1f}% | 间隔: {CHECK_INTERVAL}s")
        log.info("Ctrl+C 停止\n")

        try:
            while True:
                self._tick()
                time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            log.info("\n已停止")

    def _tick(self):
        self.cycle += 1
        try:
            rates = self.pool_reader.get_rates()
        except Exception as e:
            log.warning(f"[#{self.cycle}] 读取失败: {e}")
            return

        if len(rates) < 3:
            log.warning(f"[#{self.cycle}] 汇率不完整: {rates}")
            return

        rate_str = " | ".join(f"{k}: {v:.4f}" for k, v in rates.items())
        log.info(f"[#{self.cycle}] {rate_str}")

        opps = self.engine.find_opportunities(rates)
        best = opps[0] if opps else None
        if not best:
            return

        path_str = " → ".join(best["path"])
        log.info(f"  最优: {path_str} | 利润={best['profit']*100:+.3f}%")

        if best["profit"] > MIN_PROFIT:
            log.info(f"  ★ 套利！执行中...")
            self.executor.execute_path(best["path"])


# ============================================================
# 命令行入口
# ============================================================

def cmd_discover():
    kp = load_keypair()
    rpc = Client(RPC_URL)
    log.info(f"钱包地址: {kp.pubkey()}")
    disc = PDADiscovery(rpc, kp.pubkey())
    pdas = disc.discover()
    if pdas:
        PDA_FILE.write_text(json.dumps(pdas, indent=2))
        log.info(f"PDA 已保存到 {PDA_FILE}")


def cmd_rates():
    rpc = Client(RPC_URL)
    reader = PoolReader(rpc)
    rates = reader.get_rates()
    print("\n当前汇率:")
    for pair, rate in rates.items():
        print(f"  {pair}: {rate:.6f}")

    engine = ArbitrageEngine()
    opps = engine.find_opportunities(rates)
    print("\n套利分析:")
    for o in opps[:4]:
        print(f"  {' → '.join(o['path'])}: {o['profit']*100:+.3f}%")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()
    if cmd == "discover":
        cmd_discover()
    elif cmd == "rates":
        cmd_rates()
    elif cmd == "monitor":
        live = "--live" in sys.argv
        Bot(dry_run=not live).run()
    else:
        print(f"未知命令: {cmd}\n可用: discover | rates | monitor [--live]")


if __name__ == "__main__":
    main()
