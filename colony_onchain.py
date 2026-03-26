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
import random
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.instruction import Instruction, AccountMeta
from solders.transaction import Transaction
from solders.message import Message

# ============================================================
# 常量
# ============================================================

RPC_URL = "https://as.magicblock.app/"
RESOURCE_PROGRAM = Pubkey.from_string("2K2374VEqxbFJWycxoj8ub2wBk7KwwnNn7M5V7QsL9r2")
POOL_STATE = Pubkey.from_string("AdQJrDXwWAeBPc254qnLBCWfyTqJqoAahRgZ4kok3PZD")

SWAP_DISCRIMINATOR = bytes.fromhex("8dac0ad04509389a")
COLLECT_DISCRIMINATOR = bytes.fromhex("49047707f2ff1de2")

METAL = 0
GAS = 1
CRYSTAL = 2
RESOURCE_NAMES = {0: "Metal", 1: "Gas", 2: "Crystal"}
RESOURCE_BALANCE_NAMES = ["Metal", "Gas", "Crystal", "Stardust"]

# 交易参数
FEE = 0.003
MIN_PROFIT = 0.0
CHECK_INTERVAL_MIN = 1
CHECK_INTERVAL_MAX = 2
SWAP_AMOUNT = 1000
TRADE_RATIO = 0.30
REBALANCE_TRADE_RATIO = 0.20
MIN_TRADE_AMOUNT = 100
MAX_BALANCE_PROBE = 5_000_000
TARGET_RESOURCE_WEIGHTS = {
    "Metal": 1 / 3,
    "Gas": 1 / 3,
    "Crystal": 1 / 3,
}
MAX_RESOURCE_WEIGHT = 0.55
LOW_RESOURCE_WEIGHT = 0.05
LOW_RESOURCE_BUY_BONUS = 0.0005
LOW_RESOURCE_SELL_PENALTY = 0.020
LOW_RESOURCE_MIN_PROFIT = -0.001
MAX_CANDIDATES_TO_PROBE = 3

# 文件路径
BASE_DIR = Path(__file__).parent
ENV_FILE = BASE_DIR / ".env"
PDA_FILE = BASE_DIR / "user_pdas.json"
LOG_FILE = BASE_DIR / "onchain_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("colony_onchain")
logging.getLogger("httpx").setLevel(logging.WARNING)

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
    load_env()
    pk = os.environ.get("COLONY_PRIVATE_KEY")
    if pk:
        pk = pk.strip()
        # 策略1: 标准 base58 keypair
        try:
            kp = Keypair.from_base58_string(pk)
            log.info(f"密钥加载成功，公钥: {kp.pubkey()}")
            return kp
        except ValueError as e:
            log.info(f"标准加载失败({e})，尝试其他方式...")

        raw = b58decode(pk)
        log.info(f"key 字符数: {len(pk)}, 解码字节数: {len(raw)}")

        # 策略2~4: 尝试不同的字节切分方式
        attempts = []
        if len(raw) >= 64:
            attempts.append(("前64字节", raw[:64]))
            attempts.append(("后64字节", raw[-64:]))
        if len(raw) >= 32:
            attempts.append(("前32字节作为seed", raw[:32]))

        for desc, data in attempts:
            try:
                if len(data) == 32:
                    kp = Keypair.from_seed(data)
                else:
                    kp = Keypair.from_bytes(data)
                log.info(f"密钥加载成功（{desc}），公钥: {kp.pubkey()}")
                return kp
            except Exception:
                continue

        log.error(f"所有密钥加载方式均失败，原始字节数: {len(raw)}")
        sys.exit(1)

    path = os.environ.get("COLONY_KEYPAIR_PATH")
    if path and Path(path).exists():
        data = json.loads(Path(path).read_text())
        return Keypair.from_bytes(bytes(data))

    print("请在 .env 文件中填入私钥:")
    print(f"  文件路径: {ENV_FILE}")
    print("  格式: COLONY_PRIVATE_KEY=你的base58私钥")
    sys.exit(1)


def normalize_pdas(pdas: dict) -> dict:
    """兼容旧 discover 输出，并补齐语义化账户名。"""
    aliases = {
        "owner": "signer",
        "planet_state": "player_entity",
        "planet_nft": "player_component",
        "trading_pools": "pool_state",
        "season": "player_data",
        "session_token": "user_state",
    }
    normalized = dict(pdas)
    for canonical, legacy in aliases.items():
        if canonical not in normalized and legacy in normalized:
            normalized[canonical] = normalized[legacy]
        if legacy not in normalized and canonical in normalized:
            normalized[legacy] = normalized[canonical]
    return normalized


# ============================================================
# 链上数据读取
# ============================================================

class PoolReader:

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
        [40:42] Header (u16)
        [42:120] 3 组 TradingPool, 每组 26 bytes:
            u8  resource_a
            u8  resource_b
            u64 reserve_a
            u64 reserve_b
            u64 k
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


class PlanetStateReader:
    """读取并解析玩家的 planet_state 账户。"""

    def __init__(self, rpc: Client, pdas: dict):
        self.rpc = rpc
        self.pdas = normalize_pdas(pdas)

    def read(self) -> dict:
        planet_state = Pubkey.from_string(self.pdas["planet_state"])
        acct = self.rpc.get_account_info(planet_state, commitment=Confirmed)
        if not acct.value:
            raise RuntimeError("无法读取 planet_state 账户")

        data = bytes(acct.value.data)
        if len(data) < 236:
            raise RuntimeError(f"planet_state 长度异常: {len(data)}")

        offset = 8  # 跳过 Anchor discriminator
        season_id = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        leaderboard_points = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        planet_mint = str(Pubkey.from_bytes(data[offset : offset + 32]))
        offset += 32
        initializer = str(Pubkey.from_bytes(data[offset : offset + 32]))
        offset += 32
        planet_type = data[offset]
        offset += 1
        created_ts = struct.unpack_from("<q", data, offset)[0]
        offset += 8
        resources = [
            struct.unpack_from("<Q", data, offset + i * 8)[0]
            for i in range(4)
        ]
        offset += 32
        last_claimed_ts = struct.unpack_from("<q", data, offset)[0]
        offset += 8

        buildings = []
        for _ in range(9):
            slot_resource = data[offset]
            level = data[offset + 1]
            last_upgrade_ts = struct.unpack_from("<q", data, offset + 2)[0]
            offset += 10
            buildings.append({
                "resource": slot_resource,
                "level": level,
                "last_upgrade_ts": last_upgrade_ts,
            })

        energy = data[offset]
        offset += 1
        last_energy_ts = struct.unpack_from("<q", data, offset)[0]
        offset += 8
        mine_nonce = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        stardust_exp = struct.unpack_from("<Q", data, offset)[0]

        return {
            "season_id": season_id,
            "leaderboard_points": leaderboard_points,
            "planet_mint": planet_mint,
            "initializer": initializer,
            "planet_type": planet_type,
            "created_ts": created_ts,
            "resources": dict(zip(RESOURCE_BALANCE_NAMES, resources)),
            "last_claimed_ts": last_claimed_ts,
            "buildings": buildings,
            "energy": energy,
            "last_energy_ts": last_energy_ts,
            "mine_nonce": mine_nonce,
            "stardust_exp": stardust_exp,
        }


class InventoryManager:
    """以资源总和为主目标，仓位只做轻量修正。"""

    UTILITY_RESOURCES = ("Metal", "Gas", "Crystal")

    def summarize(self, resources: dict) -> dict:
        utility = {name: int(resources.get(name, 0)) for name in self.UTILITY_RESOURCES}
        total = sum(utility.values())
        if total <= 0:
            weights = {name: 0.0 for name in self.UTILITY_RESOURCES}
        else:
            weights = {name: utility[name] / total for name in self.UTILITY_RESOURCES}

        deviations = {
            name: weights[name] - TARGET_RESOURCE_WEIGHTS[name]
            for name in self.UTILITY_RESOURCES
        }
        return {
            "balances": utility,
            "total": total,
            "weights": weights,
            "deviations": deviations,
        }

    def evaluate_trade(self, sell: str, buy: str, profit: float, summary: dict) -> Optional[dict]:
        sell_weight = summary["weights"].get(sell, 0.0)
        buy_weight = summary["weights"].get(buy, 0.0)
        buy_is_low = buy_weight <= LOW_RESOURCE_WEIGHT
        sell_is_low = sell_weight <= LOW_RESOURCE_WEIGHT
        low_resource_replenish = buy_is_low and sell_weight > buy_weight

        blocked = buy_weight >= MAX_RESOURCE_WEIGHT and not buy_is_low
        if blocked:
            return None

        priority_bonus = LOW_RESOURCE_BUY_BONUS if low_resource_replenish else 0.0
        if sell_is_low:
            priority_bonus -= LOW_RESOURCE_SELL_PENALTY

        if low_resource_replenish:
            min_profit = LOW_RESOURCE_MIN_PROFIT
            ratio = REBALANCE_TRADE_RATIO
        else:
            min_profit = MIN_PROFIT
            ratio = TRADE_RATIO

        if profit <= min_profit:
            return None

        return {
            "profit": profit,
            "priority_bonus": priority_bonus,
            "rebalancing": low_resource_replenish,
            "forced": False,
            "replenish": low_resource_replenish,
            "sell_weight": sell_weight,
            "buy_weight": buy_weight,
            "ratio": ratio,
            "sell_balance": summary["balances"].get(sell, 0),
        }


# ============================================================
# PDA 发现
# ============================================================

class PDADiscovery:

    def __init__(self, rpc: Client, user: Pubkey):
        self.rpc = rpc
        self.user = user

    def discover(self) -> dict:
        log.info(f"钱包地址: {self.user}")
        pdas = self._search_pool_txs()
        if pdas:
            return pdas
        pdas = self._search_user_txs()
        if pdas:
            return pdas
        log.warning("未找到 swap 交易记录")
        log.warning("请先在游戏中手动执行一次 swap，然后重新运行 discover")
        return {}

    def _search_pool_txs(self) -> Optional[dict]:
        log.info("搜索 pool 账户交易记录...")
        sigs = self.rpc.get_signatures_for_address(POOL_STATE, limit=200)
        user_str = str(self.user)
        for sig_info in sigs.value:
            pdas = self._check_tx(sig_info.signature, user_str)
            if pdas:
                return pdas
        return None

    def _search_user_txs(self) -> Optional[dict]:
        log.info("搜索用户钱包交易记录...")
        sigs = self.rpc.get_signatures_for_address(self.user, limit=200)
        user_str = str(self.user)
        for sig_info in sigs.value:
            pdas = self._check_tx(sig_info.signature, user_str)
            if pdas:
                return pdas
        return None

    def _check_tx(self, signature, user_str: str) -> Optional[dict]:
        """从真实交易中提取 PDA 地址和 writable 标记"""
        tx = self.rpc.get_transaction(
            signature, max_supported_transaction_version=0
        )
        if not tx.value:
            return None

        msg = tx.value.transaction.transaction.message
        acct_keys = [str(k) for k in msg.account_keys]

        if user_str not in acct_keys:
            return None

        # 解析 message header 确定每个 account 的 writable 属性
        n_sigs = msg.header.num_required_signatures
        n_ro_signed = msg.header.num_readonly_signed_accounts
        n_ro_unsigned = msg.header.num_readonly_unsigned_accounts
        n_total = len(acct_keys)

        def is_writable(idx):
            is_signer = idx < n_sigs
            if is_signer:
                return idx < n_sigs - n_ro_signed
            return idx < n_total - n_ro_unsigned

        # 先找 swap 指令
        swap_ix = None
        collect_ix = None
        for ix in msg.instructions:
            raw = b58decode(ix.data)
            if raw[:8] == SWAP_DISCRIMINATOR:
                swap_ix = ix
            elif raw[:8] == COLLECT_DISCRIMINATOR:
                collect_ix = ix

        if not swap_ix:
            return None

        # 提取 swap 指令的 PDA 和 writable 标记
        swap_names = ["owner", "planet_state", "planet_nft",
                      "trading_pools", "season", "session_token"]
        pdas = {}
        swap_writable = {}
        for j, idx in enumerate(swap_ix.accounts):
            if j >= len(swap_names):
                break
            name = swap_names[j]
            pdas[name] = acct_keys[idx]
            swap_writable[name] = is_writable(idx)
        pdas["_writable"] = swap_writable

        # 提取 collect 指令的 writable 标记
        if collect_ix:
            collect_names = ["owner", "planet_state", "planet_nft",
                            "season", "session_token"]
            collect_writable = {}
            for j, idx in enumerate(collect_ix.accounts):
                if j >= len(collect_names):
                    break
                collect_writable[collect_names[j]] = is_writable(idx)
            pdas["_collect_writable"] = collect_writable
            log.info("找到 collect 指令 writable 标记")

        log.info("找到 PDA（含 writable 标记）:")
        for name in swap_names:
            w = "W" if swap_writable.get(name) else "R"
            log.info(f"  {name}: {pdas[name]}  [{w}]")
        return normalize_pdas(pdas)



# ============================================================
# 交易构建 & 发送
# ============================================================

class SwapExecutor:
    RES_MAP = {"Metal": 0, "Gas": 1, "Crystal": 2}

    def __init__(self, rpc: Client, keypair: Keypair, pdas: dict, dry_run=True):
        self.rpc = rpc
        self.keypair = keypair
        self.pdas = normalize_pdas(pdas)
        self.dry_run = dry_run
        self.swap_w = pdas.get("_writable", {})
        self.collect_w = pdas.get("_collect_writable", {})

    def _build_collect_ix(self) -> Instruction:
        w = self.collect_w
        accounts = [
            AccountMeta(self.keypair.pubkey(),
                        is_signer=True,
                        is_writable=w.get("owner", w.get("signer", True))),
            AccountMeta(Pubkey.from_string(self.pdas["planet_state"]),
                        is_signer=False,
                        is_writable=w.get("planet_state", w.get("player_entity", False))),
            AccountMeta(Pubkey.from_string(self.pdas["planet_nft"]),
                        is_signer=False,
                        is_writable=w.get("planet_nft", w.get("player_component", False))),
            AccountMeta(Pubkey.from_string(self.pdas["season"]),
                        is_signer=False,
                        is_writable=w.get("season", w.get("player_data", False))),
            AccountMeta(Pubkey.from_string(self.pdas["session_token"]),
                        is_signer=False,
                        is_writable=w.get("session_token", w.get("user_state", False))),
        ]
        return Instruction(RESOURCE_PROGRAM, COLLECT_DISCRIMINATOR, accounts)

    def _build_swap_ix(self, sell_type: int, buy_type: int, amount: int) -> Instruction:
        swap_data = (SWAP_DISCRIMINATOR
                     + struct.pack('<BB', sell_type, buy_type)
                     + struct.pack('<Q', amount)
                     + struct.pack('<Q', 0))
        w = self.swap_w
        accounts = [
            AccountMeta(self.keypair.pubkey(),
                        is_signer=True,
                        is_writable=w.get("owner", w.get("signer", True))),
            AccountMeta(Pubkey.from_string(self.pdas["planet_state"]),
                        is_signer=False,
                        is_writable=w.get("planet_state", w.get("player_entity", False))),
            AccountMeta(Pubkey.from_string(self.pdas["planet_nft"]),
                        is_signer=False,
                        is_writable=w.get("planet_nft", w.get("player_component", False))),
            AccountMeta(POOL_STATE,
                        is_signer=False,
                        is_writable=w.get("trading_pools", w.get("pool_state", True))),
            AccountMeta(Pubkey.from_string(self.pdas["season"]),
                        is_signer=False,
                        is_writable=w.get("season", w.get("player_data", True))),
            AccountMeta(Pubkey.from_string(self.pdas["session_token"]),
                        is_signer=False,
                        is_writable=w.get("session_token", w.get("user_state", True))),
        ]
        return Instruction(RESOURCE_PROGRAM, swap_data, accounts)

    def _build_swap_instructions(self, sell: str, buy: str, amount: int) -> List[Instruction]:
        sell_type = self.RES_MAP[sell]
        buy_type = self.RES_MAP[buy]
        return [
            self._build_collect_ix(),
            self._build_swap_ix(sell_type, buy_type, amount),
        ]

    def _simulate_swap(self, sell: str, buy: str, amount: int) -> tuple[bool, str]:
        instructions = self._build_swap_instructions(sell, buy, amount)
        try:
            blockhash = self.rpc.get_latest_blockhash(Confirmed).value.blockhash
            msg = Message.new_with_blockhash(instructions, self.keypair.pubkey(), blockhash)
            tx = Transaction.new_unsigned(msg)
            tx.sign([self.keypair], blockhash)
            sim = self.rpc.simulate_transaction(tx)
        except Exception as e:
            return False, f"模拟异常: {e}"

        if sim.value.err:
            detail = str(sim.value.err)
            logs = sim.value.logs or []
            if logs:
                detail = f"{detail} | {' | '.join(logs[-3:])}"
            return False, detail
        return True, "ok"

    def can_trade_amount(self, sell: str, buy: str, amount: int) -> tuple[bool, str]:
        return self._simulate_swap(sell, buy, amount)

    def estimate_tradable_amount(self, sell: str, buy: str) -> int:
        """
        用模拟交易估算当前 sell→buy 方向可成交的上限。
        这不是账户原始余额字段，而是综合用户余额和池子流动性后的可交易额度。
        """
        ok, detail = self._simulate_swap(sell, buy, MIN_TRADE_AMOUNT)
        if not ok:
            log.warning(f"  无法读取可交易额度，{MIN_TRADE_AMOUNT} 单位模拟失败: {detail}")
            return 0

        low = MIN_TRADE_AMOUNT
        high = max(SWAP_AMOUNT, MIN_TRADE_AMOUNT)
        while high < MAX_BALANCE_PROBE:
            ok, _ = self._simulate_swap(sell, buy, high)
            if not ok:
                break
            low = high
            high *= 2

        if high >= MAX_BALANCE_PROBE:
            ok, _ = self._simulate_swap(sell, buy, MAX_BALANCE_PROBE)
            if ok:
                return MAX_BALANCE_PROBE

        left = low + 1
        right = min(high - 1, MAX_BALANCE_PROBE)
        best = low
        while left <= right:
            mid = (left + right) // 2
            ok, _ = self._simulate_swap(sell, buy, mid)
            if ok:
                best = mid
                left = mid + 1
            else:
                right = mid - 1
        return best

    def plan_trade_amount(self, sell: str, buy: str, trade_ratio: float = TRADE_RATIO) -> tuple[int, int]:
        available = self.estimate_tradable_amount(sell, buy)
        if available <= 0:
            return 0, 0

        amount = int(available * trade_ratio)
        amount = min(amount, available)
        if available >= MIN_TRADE_AMOUNT:
            amount = max(amount, MIN_TRADE_AMOUNT)
        return amount, available

    def execute_swap(self, sell: str, buy: str, amount: int = SWAP_AMOUNT) -> Optional[str]:
        instructions = self._build_swap_instructions(sell, buy, amount)

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
                    for line in (sim.value.logs or []):
                        log.warning(f"    {line}")
                    return None
                else:
                    log.info(f"  模拟成功 ✓")
                    return "simulated"
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
            return sig
        except Exception as e:
            log.error(f"  发送失败: {e}")
            return None


# ============================================================
# Bot 主循环
# ============================================================

class Bot:

    def __init__(self, dry_run=True):
        self.keypair = load_keypair()
        self.rpc = Client(RPC_URL)

        if not PDA_FILE.exists():
            log.error("请先运行: python colony_onchain.py discover")
            sys.exit(1)
        self.pdas = normalize_pdas(json.loads(PDA_FILE.read_text()))

        # 校验密钥与 signer 匹配
        expected = self.pdas.get("owner", self.pdas.get("signer", ""))
        actual = str(self.keypair.pubkey())
        if expected and actual != expected:
            log.error(f"密钥不匹配!")
            log.error(f"  加载的公钥: {actual}")
            log.error(f"  PDA signer: {expected}")
            log.error("请检查 .env 中的私钥是否正确")
            sys.exit(1)

        self.pool_reader = PoolReader(self.rpc)
        self.planet_state_reader = PlanetStateReader(self.rpc, self.pdas)
        self.inventory = InventoryManager()
        self.executor = SwapExecutor(self.rpc, self.keypair, self.pdas, dry_run)
        self.dry_run = dry_run
        self.cycle = 0

    def run(self):
        mode = "DRY-RUN" if self.dry_run else "⚡ LIVE"
        log.info(f"启动链上套利监控 [{mode}]")
        log.info(f"钱包: {self.keypair.pubkey()}")
        log.info(
            f"阈值: >{FEE*100:.1f}%手续费 | 间隔: {CHECK_INTERVAL_MIN}-{CHECK_INTERVAL_MAX}s | "
            f"下单比例: {TRADE_RATIO*100:.0f}%/{REBALANCE_TRADE_RATIO*100:.0f}% | "
            f"最小下单: {MIN_TRADE_AMOUNT} | 候选探测数: {MAX_CANDIDATES_TO_PROBE}"
        )
        log.info("Ctrl+C 停止\n")

        try:
            while True:
                self._tick()
                delay = random.uniform(CHECK_INTERVAL_MIN, CHECK_INTERVAL_MAX)
                time.sleep(delay)
        except KeyboardInterrupt:
            log.info("\n已停止")

    def _tick(self):
        self.cycle += 1
        try:
            rates = self.pool_reader.get_rates()
            state = self.planet_state_reader.read()
        except Exception as e:
            log.warning(f"[#{self.cycle}] 读取失败: {e}")
            return

        if len(rates) < 3:
            log.warning(f"[#{self.cycle}] 汇率不完整: {rates}")
            return

        summary = self.inventory.summarize(state["resources"])
        before_total = summary["total"]
        rate_str = " | ".join(f"{k}: {v:.4f}" for k, v in rates.items())
        log.info(f"[#{self.cycle}] {rate_str}")
        weight_str = " | ".join(
            f"{name}:{summary['balances'][name]}({summary['weights'][name]*100:.1f}%)"
            for name in InventoryManager.UTILITY_RESOURCES
        )
        log.info(f"  仓位: {weight_str}")

        # 收集所有以总和收益为主、仓位为辅的交易机会
        fee_mul = 1 - FEE
        opportunities = []
        for pair, rate in rates.items():
            res_a, res_b = pair.split("_")
            net_ab = rate * fee_mul
            net_ba = (1.0 / rate) * fee_mul

            profit = net_ab - 1
            decision = self.inventory.evaluate_trade(res_a, res_b, profit, summary)
            if decision:
                opportunities.append((profit * 100, res_a, res_b, decision))

            profit = net_ba - 1
            decision = self.inventory.evaluate_trade(res_b, res_a, profit, summary)
            if decision:
                opportunities.append((profit * 100, res_b, res_a, decision))

        if not opportunities:
            log.info("  当前没有兼顾利润和仓位平衡的可执行机会")
            return

        rough_ranked = []
        for raw_profit, sell, buy, decision in opportunities:
            rough_available = max(0, decision["sell_balance"])
            rough_amount = min(
                rough_available,
                max(MIN_TRADE_AMOUNT, int(rough_available * decision["ratio"])) if rough_available else 0,
            )
            rough_edge = rough_amount * (raw_profit / 100)
            rough_ranked.append((rough_edge, decision["priority_bonus"], raw_profit, sell, buy, decision))

        rough_ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
        rough_ranked = rough_ranked[:MAX_CANDIDATES_TO_PROBE]

        scored_opportunities = []
        for _, _, raw_profit, sell, buy, decision in rough_ranked:
            min_probe, min_probe_detail = self.executor.can_trade_amount(sell, buy, MIN_TRADE_AMOUNT)
            if not min_probe:
                scored_opportunities.append({
                    "sell": sell,
                    "buy": buy,
                    "decision": decision,
                    "raw_profit": raw_profit,
                    "liquid": False,
                    "reason": min_probe_detail,
                })
                continue

            amount, available = self.executor.plan_trade_amount(
                sell, buy, trade_ratio=decision["ratio"]
            )
            if amount < MIN_TRADE_AMOUNT:
                scored_opportunities.append({
                    "sell": sell,
                    "buy": buy,
                    "decision": decision,
                    "raw_profit": raw_profit,
                    "liquid": False,
                    "reason": "计划数量低于最小下单量",
                })
                continue

            expected_edge = amount * (raw_profit / 100)
            scored_opportunities.append({
                "sell": sell,
                "buy": buy,
                "decision": decision,
                "raw_profit": raw_profit,
                "amount": amount,
                "available": available,
                "expected_edge": expected_edge,
                "liquid": True,
            })

        executable = [item for item in scored_opportunities if item["liquid"]]
        if not executable:
            for item in scored_opportunities:
                if item["decision"]["replenish"]:
                    tag = "补仓"
                elif item["decision"]["rebalancing"]:
                    tag = "再平衡"
                else:
                    tag = "套利"
                log.info(f"  {item['sell']}→{item['buy']} {tag}方向当前不可执行")
                if "InsufficientPoolLiquidity" in item["reason"]:
                    log.info("  原因: 池子在这个方向上接不住最小交易量")
                else:
                    log.info(f"  原因: {item['reason']}")
            log.info("  当前没有兼顾总和收益、轻量补仓和成交量的可执行机会")
            return

        # 按预计总和收益额优先，补仓偏好只作为次级排序
        executable.sort(
            key=lambda item: (item["expected_edge"], item["decision"]["priority_bonus"]),
            reverse=True,
        )

        # 依次尝试，失败就换下一个
        for item in executable:
            sell = item["sell"]
            buy = item["buy"]
            decision = item["decision"]
            raw_profit = item["raw_profit"]
            amount = item["amount"]
            available = item["available"]
            if decision["replenish"]:
                tag = "补仓"
            elif decision["rebalancing"]:
                tag = "再平衡"
            else:
                tag = "套利"
            log.info(
                f"  ★ {sell}→{buy} 总和收益率 {raw_profit:.2f}% | "
                f"预计收益额 {item['expected_edge']:.1f} | {tag}"
            )

            if available >= MAX_BALANCE_PROBE:
                available_text = f">={MAX_BALANCE_PROBE}"
            else:
                available_text = str(available)
            log.info(
                f"  {sell}→{buy} 当前可交易额度约 {available_text}，"
                f"仓位 {decision['sell_weight']*100:.1f}%→{decision['buy_weight']*100:.1f}% ，"
                f"本次下单 {amount}"
            )

            result = self.executor.execute_swap(sell, buy, amount=amount)
            if result:
                self._log_realized_edge(before_total, sell, buy, amount)
                return
            log.info(f"  {sell}→{buy} 失败，尝试下一对...")

    def _log_realized_edge(self, before_total: int, sell: str, buy: str, amount: int):
        if self.dry_run:
            log.info("  实际收益额: DRY-RUN 模式不计算")
            return

        try:
            time.sleep(1.0)
            after_state = self.planet_state_reader.read()
        except Exception as e:
            log.warning(f"  无法读取交易后余额，实际收益额未计算: {e}")
            return

        after_summary = self.inventory.summarize(after_state["resources"])
        after_total = after_summary["total"]
        realized_edge = after_total - before_total

        log.info(
            f"  实际收益额(交易后总数-交易前总数): {realized_edge}"
        )
        log.info(
            f"  交易后余额: Metal={after_summary['balances']['Metal']} | "
            f"Gas={after_summary['balances']['Gas']} | "
            f"Crystal={after_summary['balances']['Crystal']}"
        )


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
    fee_mul = 1 - FEE
    print("\n当前汇率:")
    for pair, rate in rates.items():
        res_a, res_b = pair.split("_")
        net = rate * fee_mul
        inv_net = (1.0 / rate) * fee_mul
        status_ab = f"+{(net-1)*100:.2f}%" if net > 1 else f"{(net-1)*100:.2f}%"
        status_ba = f"+{(inv_net-1)*100:.2f}%" if inv_net > 1 else f"{(inv_net-1)*100:.2f}%"
        print(f"  {res_a}/{res_b}: {rate:.6f}  卖{res_a}买{res_b}={status_ab}  卖{res_b}买{res_a}={status_ba}")


def cmd_balances():
    kp = load_keypair()
    rpc = Client(RPC_URL)

    if not PDA_FILE.exists():
        print("user_pdas.json 不存在，请先运行 discover")
        return

    pdas = normalize_pdas(json.loads(PDA_FILE.read_text()))
    expected = pdas.get("owner", pdas.get("signer", ""))
    actual = str(kp.pubkey())
    if expected and actual != expected:
        print("密钥不匹配，无法读取当前钱包对应的 planet_state")
        print(f"  加载的公钥: {actual}")
        print(f"  PDA owner:   {expected}")
        return

    state = PlanetStateReader(rpc, pdas).read()
    print("\n当前链上余额（planet_state.resources）:")
    for name in RESOURCE_BALANCE_NAMES:
        print(f"  {name}: {state['resources'][name]}")

    print(f"\n附加信息:")
    print(f"  season_id: {state['season_id']}")
    print(f"  leaderboard_points: {state['leaderboard_points']}")
    print(f"  planet_type: {state['planet_type']}")
    print(f"  energy: {state['energy']}")
    print(f"  stardust_exp: {state['stardust_exp']}")
    print(f"  planet_mint: {state['planet_mint']}")
    print("\n说明: 这里读取的是链上已记账资源；若游戏界面有尚未 claim 的产出，页面数值可能更高。")


def cmd_verify():
    """诊断密钥和交易"""
    kp = load_keypair()
    rpc = Client(RPC_URL)

    print(f"\n1. 加载的公钥: {kp.pubkey()}")

    if PDA_FILE.exists():
        pdas = normalize_pdas(json.loads(PDA_FILE.read_text()))
        signer = pdas.get("owner", pdas.get("signer", ""))
        match = "[OK] 匹配" if str(kp.pubkey()) == signer else "[FAIL] 不匹配!"
        print(f"   PDA signer:  {signer}")
        print(f"   状态: {match}")

        if str(kp.pubkey()) != signer:
            print("\n   密钥不匹配！.env 中的私钥与游戏内 session key 不一致。")
            print("   请重新从浏览器 IndexedDB 中提取正确的 session.privatekey")
            return

        w = pdas.get("_writable", {})
        print(f"\n2. writable 标记: {json.dumps(w)}")

        state = PlanetStateReader(rpc, pdas).read()
        print(f"\n3. 当前链上余额:")
        for name in RESOURCE_BALANCE_NAMES:
            print(f"   {name}: {state['resources'][name]}")

        print(f"\n4. 模拟 swap Metal→Gas...")
        exe = SwapExecutor(rpc, kp, pdas, dry_run=True)
        result = exe.execute_swap("Metal", "Gas", 100)
        print(f"   结果: {'成功' if result else '失败'}")

        print(f"\n5. 估算当前可交易额度...")
        for sell, buy in [("Metal", "Gas"), ("Gas", "Crystal"), ("Crystal", "Metal")]:
            amount = exe.estimate_tradable_amount(sell, buy)
            print(f"   {sell}→{buy}: 约 {amount}")
    else:
        print("   user_pdas.json 不存在，请先运行 discover")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()
    if cmd == "discover":
        cmd_discover()
    elif cmd == "balances":
        cmd_balances()
    elif cmd == "rates":
        cmd_rates()
    elif cmd == "verify":
        cmd_verify()
    elif cmd == "monitor":
        live = "--live" in sys.argv
        Bot(dry_run=not live).run()
    else:
        print(f"未知命令: {cmd}\n可用: discover | balances | rates | verify | monitor [--live]")


if __name__ == "__main__":
    main()
