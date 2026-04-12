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
import math
import struct
import time
import sys
import os
import logging
import random
import re
import statistics
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
TRADE_RATIO = 0.15
MIN_TRADE_AMOUNT = 100
MAX_BALANCE_PROBE = 5_000_000
MAX_CANDIDATES_TO_PROBE = 3
OPEN_SPREAD_THRESHOLD = 0.01
CLOSE_PROFIT_TARGET = 0.01
REBALANCE_INTERVAL = 3600
REBALANCE_DEVIATION = 0.10
MINE_SCAN_PAGES = 5
MINE_SCAN_PAGE_SIZE = 100
CRIT_MULTIPLIER_THRESHOLD = 1000

# 文件路径
BASE_DIR = Path(__file__).parent
ENV_FILE = BASE_DIR / ".env"
PDA_FILE = BASE_DIR / "user_pdas.json"
LOG_FILE = BASE_DIR / "onchain_log.txt"
POSITIONS_FILE = BASE_DIR / "swap_positions.json"
TRADE_HISTORY_FILE = BASE_DIR / "trade_history.json"
TRADE_LOG_MD = BASE_DIR / "trade_log.md"

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

    def __init__(self, rpc: Client, pool_pubkey: Pubkey = None):
        self.rpc = rpc
        self.pool_pubkey = pool_pubkey or POOL_STATE

    def read_pool_data(self) -> bytes:
        acct = self.rpc.get_account_info(self.pool_pubkey, commitment=Confirmed)
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
    """只负责汇总当前三种资源总量。"""

    UTILITY_RESOURCES = ("Metal", "Gas", "Crystal")

    def summarize(self, resources: dict) -> dict:
        utility = {name: int(resources.get(name, 0)) for name in self.UTILITY_RESOURCES}
        total = sum(utility.values())
        if total <= 0:
            weights = {name: 0.0 for name in self.UTILITY_RESOURCES}
        else:
            weights = {name: utility[name] / total for name in self.UTILITY_RESOURCES}

        return {
            "balances": utility,
            "total": total,
            "weights": weights,
        }

    def evaluate_trade(self, sell: str, buy: str, profit: float, summary: dict) -> Optional[dict]:
        if profit <= OPEN_SPREAD_THRESHOLD:
            return None

        return {
            "profit": profit,
            "priority_bonus": 0.0,
            "rebalancing": False,
            "forced": False,
            "replenish": False,
            "sell_weight": summary["weights"].get(sell, 0.0),
            "buy_weight": summary["weights"].get(buy, 0.0),
            "ratio": TRADE_RATIO,
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
        pdas = self._search_user_txs()
        if pdas:
            return pdas
        pdas = self._search_pool_txs()
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

        # 提取 collect 指令的 PDA 和 writable 标记
        if collect_ix:
            collect_names = ["owner", "planet_state", "planet_nft",
                            "season", "session_token", "item_slot"]
            collect_writable = {}
            for j, idx in enumerate(collect_ix.accounts):
                if j >= len(collect_names):
                    break
                name = collect_names[j]
                collect_writable[name] = is_writable(idx)
                if name == "item_slot":
                    pdas["item_slot"] = acct_keys[idx]
            pdas["_collect_writable"] = collect_writable
            log.info("找到 collect 指令 writable 标记")

        log.info("找到 PDA（含 writable 标记）:")
        for name in swap_names:
            w = "W" if swap_writable.get(name) else "R"
            log.info(f"  {name}: {pdas[name]}  [{w}]")
        if "item_slot" in pdas:
            log.info(f"  item_slot: {pdas['item_slot']}")
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
        if "item_slot" in self.pdas:
            accounts.append(
                AccountMeta(Pubkey.from_string(self.pdas["item_slot"]),
                            is_signer=False,
                            is_writable=w.get("item_slot", False)),
            )
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
            AccountMeta(Pubkey.from_string(self.pdas["trading_pools"]),
                        is_signer=False,
                        is_writable=w.get("trading_pools", w.get("pool_state", True))),
            AccountMeta(Pubkey.from_string(self.pdas["season"]),
                        is_signer=False,
                        is_writable=w.get("season", w.get("player_data", False))),
            AccountMeta(Pubkey.from_string(self.pdas["session_token"]),
                        is_signer=False,
                        is_writable=w.get("session_token", w.get("user_state", False))),
        ]
        return Instruction(RESOURCE_PROGRAM, swap_data, accounts)

    def _build_swap_instructions(self, sell: str, buy: str, amount: int) -> List[Instruction]:
        sell_type = self.RES_MAP[sell]
        buy_type = self.RES_MAP[buy]
        return [
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

    def quote_swap(self, sell: str, buy: str, amount: int) -> Optional[dict]:
        instructions = self._build_swap_instructions(sell, buy, amount)
        try:
            blockhash = self.rpc.get_latest_blockhash(Confirmed).value.blockhash
            msg = Message.new_with_blockhash(instructions, self.keypair.pubkey(), blockhash)
            tx = Transaction.new_unsigned(msg)
            tx.sign([self.keypair], blockhash)
            sim = self.rpc.simulate_transaction(tx)
        except Exception as e:
            log.warning(f"  报价模拟异常: {e}")
            return None

        if sim.value.err:
            logs = sim.value.logs or []
            detail = str(sim.value.err)
            if logs:
                detail = f"{detail} | {' | '.join(logs[-3:])}"
            log.warning(f"  报价模拟失败: {detail}")
            return None

        logs = sim.value.logs or []
        for line in logs:
            match = SWAP_RESULT_RE.search(line)
            if match:
                return {
                    "amount_in": int(match.group(1)),
                    "asset_in": int(match.group(2)),
                    "amount_out": int(match.group(3)),
                    "asset_out": int(match.group(4)),
                }
        return None

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

    def execute_collect(self) -> Optional[str]:
        ix = self._build_collect_ix()
        try:
            blockhash = self.rpc.get_latest_blockhash(Confirmed).value.blockhash
            msg = Message.new_with_blockhash([ix], self.keypair.pubkey(), blockhash)
            tx = Transaction.new_unsigned(msg)
            tx.sign([self.keypair], blockhash)

            if self.dry_run:
                sim = self.rpc.simulate_transaction(tx)
                if sim.value.err:
                    log.debug(f"  Collect 模拟失败: {sim.value.err}")
                    return None
                return "simulated"

            sim = self.rpc.simulate_transaction(tx)
            if sim.value.err:
                log.debug(f"  Collect 模拟失败: {sim.value.err}")
                return None
            result = self.rpc.send_transaction(tx)
            return str(result.value)
        except Exception as e:
            log.debug(f"  Collect 异常: {e}")
            return None

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

            sim = self.rpc.simulate_transaction(tx)
            if sim.value.err:
                log.warning(f"  发送前模拟失败: {sim.value.err}")
                for line in (sim.value.logs or [])[-5:]:
                    log.warning(f"    {line}")
                return None
            log.info(f"  模拟通过，正在发送...")

            result = self.rpc.send_transaction(tx)
            sig = str(result.value)
            log.info(f"  交易已发送: {sig}")
            return sig
        except Exception as e:
            log.error(f"  发送失败: {e}")
            return None


MINE_DEBUG_RE = re.compile(
    r"nonce=(\d+), daily_production=(\d+), base_yield=(\d+), multiplier=(\d+), total=(\d+)"
)
MINE_RESULT_RE = re.compile(
    r"Mined resource (\d+).* yield: (\d+), energy: (\d+)"
)
SWAP_RESULT_RE = re.compile(
    r"Swapped (\d+) of asset (\d+) for (\d+) of asset (\d+)"
)


def _parse_int_arg(flag: str, default: int) -> int:
    if flag not in sys.argv:
        return default
    idx = sys.argv.index(flag)
    if idx + 1 >= len(sys.argv):
        raise ValueError(f"{flag} 缺少数值")
    return int(sys.argv[idx + 1])


def _parse_mine_row(tx_value) -> Optional[dict]:
    if not tx_value or not tx_value.transaction.meta or not tx_value.transaction.meta.log_messages:
        return None

    logs = tx_value.transaction.meta.log_messages
    joined = "\n".join(logs)
    if "Instruction: Mine" not in joined:
        return None

    debug = MINE_DEBUG_RE.search(joined)
    if not debug:
        return None

    result = MINE_RESULT_RE.search(joined)
    account_keys = tx_value.transaction.transaction.message.account_keys
    planet_state = str(account_keys[1]) if len(account_keys) > 1 else ""
    owner = str(account_keys[0]) if account_keys else ""

    return {
        "slot": tx_value.slot,
        "block_time": tx_value.block_time,
        "owner": owner,
        "planet_state": planet_state,
        "nonce": int(debug.group(1)),
        "daily_production": int(debug.group(2)),
        "base_yield": int(debug.group(3)),
        "multiplier": int(debug.group(4)),
        "total": int(debug.group(5)),
        "resource": int(result.group(1)) if result else None,
        "energy": int(result.group(3)) if result else None,
    }


def load_positions() -> List[dict]:
    if not POSITIONS_FILE.exists():
        return []
    try:
        data = json.loads(POSITIONS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        log.warning("持仓文件损坏，已忽略旧内容")
        return []
    return data if isinstance(data, list) else []


def save_positions(positions: List[dict]):
    POSITIONS_FILE.write_text(
        json.dumps(positions, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def calc_close_target_amount(amount_in: int) -> int:
    return max(MIN_TRADE_AMOUNT, math.ceil(amount_in * (1 + CLOSE_PROFIT_TARGET)))


def load_trade_history() -> List[dict]:
    if not TRADE_HISTORY_FILE.exists():
        return []
    try:
        data = json.loads(TRADE_HISTORY_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def save_trade_history(history: List[dict]):
    TRADE_HISTORY_FILE.write_text(
        json.dumps(history, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    render_trade_log_md(history)


def render_trade_log_md(history: List[dict]):
    lines = [
        "# 交易日志\n",
        "| # | 开仓时间 | 方向 | 卖出量 | 买入量 | 平仓目标 | 平仓时间 | 实际换回 | 盈亏 | 状态 |",
        "|---|----------|------|--------|--------|----------|----------|----------|------|------|",
    ]
    for i, row in enumerate(history, 1):
        sell = row.get("sell_resource", "?")
        buy = row.get("buy_resource", "?")
        direction = f"{sell}→{buy}"
        opened = row.get("opened_at", "")[:19]
        amount_in = row.get("amount_in", 0)
        amount_out = row.get("amount_out", 0)
        target = row.get("target_amount_back", 0)
        closed = row.get("closed_at", "")
        closed = closed[:19] if closed else "-"
        actual_back = row.get("actual_amount_back", "")
        actual_back = str(actual_back) if actual_back != "" else "-"
        pnl = "-"
        if row.get("actual_amount_back", "") != "":
            pnl = f"{int(row['actual_amount_back']) - int(amount_in):+d}"
        status = row.get("status", "持仓中")
        lines.append(
            f"| {i} | {opened} | {direction} | "
            f"{amount_in} {sell} | {amount_out} {buy} | "
            f"{target} {sell} | {closed} | {actual_back} | {pnl} | {status} |"
        )
    lines.append("")
    TRADE_LOG_MD.write_text("\n".join(lines), encoding="utf-8")


def cmd_analyze_mine():
    pages = _parse_int_arg("--pages", MINE_SCAN_PAGES)
    page_size = _parse_int_arg("--page-size", MINE_SCAN_PAGE_SIZE)
    crit_threshold = _parse_int_arg("--crit-threshold", CRIT_MULTIPLIER_THRESHOLD)

    rpc = Client(RPC_URL)
    rows = []
    before = None
    fetched_sigs = 0

    print("\n扫描全链 Mine 交易中...")
    print(f"  pages={pages}, page_size={page_size}, crit_threshold={crit_threshold}")

    for page in range(pages):
        sigs = rpc.get_signatures_for_address(RESOURCE_PROGRAM, before=before, limit=page_size).value
        if not sigs:
            break
        fetched_sigs += len(sigs)
        before = sigs[-1].signature

        for sig_info in sigs:
            tx = rpc.get_transaction(sig_info.signature, max_supported_transaction_version=0).value
            row = _parse_mine_row(tx)
            if row:
                row["signature"] = str(sig_info.signature)
                rows.append(row)

        print(f"  第 {page + 1} 页完成，累计签名 {fetched_sigs}，Mine 样本 {len(rows)}")

    if not rows:
        print("\n未找到 Mine 样本。")
        print("说明: 当前 RPC 可访问历史里没有解析到 Mine，或历史范围不足。")
        return

    rows.sort(key=lambda r: (r["slot"], r["nonce"]))
    multipliers = [r["multiplier"] for r in rows]
    crits = [r for r in rows if r["multiplier"] >= crit_threshold]
    highs = [r for r in rows if 115 <= r["multiplier"] < crit_threshold]
    unique_planets = len({r["planet_state"] for r in rows if r["planet_state"]})
    unique_owners = len({r["owner"] for r in rows if r["owner"]})

    print("\n全链 Mine 概况:")
    print(f"  Mine 样本数: {len(rows)}")
    print(f"  涉及 planet_state 数: {unique_planets}")
    print(f"  涉及 owner 数: {unique_owners}")
    print(f"  multiplier 最小/中位/平均/最大: {min(multipliers)} / {statistics.median(multipliers)} / {statistics.mean(multipliers):.2f} / {max(multipliers)}")
    print(f"  暴击样本数(multiplier>={crit_threshold}): {len(crits)}")
    print(f"  高倍率样本数(115-{crit_threshold - 1}): {len(highs)}")

    if crits:
        print("\n暴击样本:")
        for row in crits[:20]:
            ts = datetime.fromtimestamp(row["block_time"]).isoformat(sep=" ") if row["block_time"] else "unknown"
            print(
                f"  nonce={row['nonce']} mult={row['multiplier']} total={row['total']} "
                f"resource={row['resource']} energy={row['energy']} time={ts}"
            )
            print(f"    sig={row['signature']}")
    else:
        print("\n暴击样本: 当前扫描范围内没有命中。")

    if highs:
        print("\n高倍率样本(>=115):")
        for row in highs[:20]:
            print(
                f"  nonce={row['nonce']} mult={row['multiplier']} total={row['total']} "
                f"resource={row['resource']} energy={row['energy']}"
            )

    high_nonce_mod = [(r["nonce"] % 10, r["nonce"], r["multiplier"]) for r in highs + crits]
    if high_nonce_mod:
        print("\n高倍率 nonce 尾数:")
        print("  " + ", ".join(f"{mod}->{nonce}/{mult}" for mod, nonce, mult in high_nonce_mod[:40]))

    print("\n初步判断:")
    if len(crits) <= 1:
        print("  暴击样本太少，还看不出稳定规律。")
        print("  现阶段更像是程序内部随机结果，而不是固定 nonce 周期。")
    else:
        print("  已有多个暴击样本，可以进一步检验是否和 nonce、energy、resource 有相关性。")
    print("  如果要继续逼近规律，建议把 pages 提高到 20 以上，扩大样本。")


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

        pool_pubkey = Pubkey.from_string(self.pdas["trading_pools"])
        self.pool_reader = PoolReader(self.rpc, pool_pubkey)
        self.planet_state_reader = PlanetStateReader(self.rpc, self.pdas)
        self.inventory = InventoryManager()
        self.executor = SwapExecutor(self.rpc, self.keypair, self.pdas, dry_run)
        self.dry_run = dry_run
        self.cycle = 0
        self.positions = load_positions()
        self.last_rebalance_time = 0

    def run(self):
        mode = "DRY-RUN" if self.dry_run else "⚡ LIVE"
        log.info(f"启动链上套利监控 [{mode}]")
        log.info(f"钱包: {self.keypair.pubkey()}")
        log.info(
            f"阈值: >{FEE*100:.1f}%手续费 | 间隔: {CHECK_INTERVAL_MIN}-{CHECK_INTERVAL_MAX}s | "
            f"每次交易余额比例: {TRADE_RATIO*100:.0f}% | 最小下单: {MIN_TRADE_AMOUNT}"
        )
        log.info(
            f"开仓条件: 总和收益率 > {OPEN_SPREAD_THRESHOLD*100:.1f}%"
        )
        log.info(
            f"平仓条件: 已买入资产按记录数量换回时，能换到不少于开仓卖出量的 {100 + CLOSE_PROFIT_TARGET*100:.1f}%"
        )
        log.info(
            f"仓位平衡: 启动时执行 + 每 {REBALANCE_INTERVAL}s | "
            f"偏差阈值: {REBALANCE_DEVIATION*100:.0f}%"
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

        # 先 Collect，把建筑累计产出结算到链上
        collect_result = self.executor.execute_collect()
        if collect_result:
            log.info(f"[#{self.cycle}] Collect 成功，资源已结算")
            time.sleep(0.5)

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
        log.info(f"  当前未平仓记录: {len(self.positions)}")
        self._log_closest_position(summary)

        now = time.time()
        if now - self.last_rebalance_time >= REBALANCE_INTERVAL:
            if self._try_rebalance(summary):
                self.last_rebalance_time = now
                return

        if self._try_close_position(summary, before_total):
            return

        self._try_open_position(rates, summary, before_total)

    def _try_rebalance(self, summary: dict) -> bool:
        balances = summary["balances"]
        total = summary["total"]
        if total < MIN_TRADE_AMOUNT * 3:
            log.info("  仓位平衡: 总资源量不足，跳过")
            return False

        target = total // 3
        max_rounds = 3
        did_trade = False

        for round_num in range(1, max_rounds + 1):
            weights = {name: balances[name] / total for name in balances if total > 0}
            max_res = max(weights, key=weights.get)
            min_res = min(weights, key=weights.get)
            deviation = weights[max_res] - weights[min_res]

            if deviation <= REBALANCE_DEVIATION:
                log.info(
                    f"  仓位平衡: 偏差 {deviation*100:.1f}% <= {REBALANCE_DEVIATION*100:.0f}%，已平衡"
                )
                break

            excess = balances[max_res] - target
            deficit = target - balances[min_res]
            amount = min(excess, deficit)
            amount = max(amount, MIN_TRADE_AMOUNT)
            amount = min(amount, balances[max_res])

            if amount < MIN_TRADE_AMOUNT:
                break

            log.info(
                f"  ★ 仓位平衡[{round_num}/{max_rounds}] {max_res}→{min_res} | "
                f"偏差 {deviation*100:.1f}% | 转移量 {amount}"
            )

            result = self.executor.execute_swap(max_res, min_res, amount=amount)
            if not result:
                log.info("  平衡交易失败，中止本轮平衡")
                break

            did_trade = True

            # 重新读取链上余额
            try:
                time.sleep(1.5)
                state = self.planet_state_reader.read()
                new_summary = self.inventory.summarize(state["resources"])
                balances = new_summary["balances"]
                total = new_summary["total"]
                target = total // 3
                weight_str = " | ".join(
                    f"{name}:{balances[name]}({balances[name]/total*100:.1f}%)"
                    for name in InventoryManager.UTILITY_RESOURCES
                )
                log.info(f"  平衡后仓位: {weight_str}")
            except Exception as e:
                log.warning(f"  读取平衡后余额失败: {e}")
                break

        if did_trade:
            log.info("  仓位平衡完成")
        return did_trade

    def _log_closest_position(self, summary: dict):
        if not self.positions:
            return

        checks = []
        for position in self.positions:
            amount_out = int(position.get("amount_out", 0))
            if amount_out < MIN_TRADE_AMOUNT:
                continue

            held = summary["balances"].get(position["buy_resource"], 0)
            if held < amount_out:
                continue

            quote = self.executor.quote_swap(
                position["buy_resource"],
                position["sell_resource"],
                amount_out,
            )
            if not quote:
                continue

            target_amount_back = calc_close_target_amount(int(position["amount_in"]))
            gap = quote["amount_out"] - target_amount_back
            checks.append((gap, position, quote, target_amount_back))

        if not checks:
            log.info("  持仓检查: 当前没有可报价的平仓候选")
            return

        gap, position, quote, target_amount_back = max(checks, key=lambda item: item[0])
        status = "已满足平仓条件" if gap >= 0 else f"还差 {-gap} {position['sell_resource']}"
        log.info(
            f"  最接近平仓: {position['buy_resource']}→{position['sell_resource']} | "
            f"{position['amount_out']} {position['buy_resource']} 现在可换 {quote['amount_out']} {position['sell_resource']} | "
            f"目标 {target_amount_back} | {status}"
        )

    def _try_close_position(self, summary: dict, before_total: int) -> bool:
        closable = []
        for idx, position in enumerate(self.positions):
            held = summary["balances"].get(position["buy_resource"], 0)
            required_amount = int(position["amount_out"])
            if held < required_amount or required_amount < MIN_TRADE_AMOUNT:
                continue

            quote = self.executor.quote_swap(
                position["buy_resource"],
                position["sell_resource"],
                required_amount,
            )
            if not quote:
                continue

            target_amount_back = calc_close_target_amount(int(position["amount_in"]))
            if quote["amount_out"] >= target_amount_back:
                closable.append({
                    "index": idx,
                    "position": position,
                    "quote": quote,
                    "surplus": quote["amount_out"] - target_amount_back,
                })

        if not closable:
            return False

        closable.sort(key=lambda item: (item["surplus"], item["quote"]["amount_out"]), reverse=True)
        item = closable[0]
        position = item["position"]
        quote = item["quote"]
        amount = int(position["amount_out"])

        log.info(
            f"  ★ 平仓 {position['buy_resource']}→{position['sell_resource']} | "
            f"{amount} {position['buy_resource']} 预计换回 {quote['amount_out']} {position['sell_resource']}"
        )

        result = self.executor.execute_swap(position["buy_resource"], position["sell_resource"], amount=amount)
        if not result:
            log.info("  平仓失败，等待下一轮")
            return False

        if not self.dry_run:
            self.positions.pop(item["index"])
            save_positions(self.positions)
            # 读取交易后链上余额，计算实际换回量
            actual_back = quote["amount_out"]
            try:
                time.sleep(1.0)
                after_state = self.planet_state_reader.read()
                sell_res = position["sell_resource"]
                before_sell = summary["balances"].get(sell_res, 0)
                after_sell = int(after_state["resources"].get(sell_res, 0))
                if after_sell > before_sell:
                    actual_back = after_sell - before_sell
                    log.info(f"  链上实际换回: {actual_back} {sell_res}")
            except Exception as e:
                log.warning(f"  读取交易后余额失败，使用报价值: {e}")
            history = load_trade_history()
            for record in history:
                if (record.get("open_tx") == position.get("open_tx")
                        and record.get("status") == "持仓中"):
                    record["closed_at"] = datetime.now().isoformat()
                    record["actual_amount_back"] = actual_back
                    record["close_tx"] = result
                    record["status"] = "已平仓"
                    break
            save_trade_history(history)
        self._log_realized_edge(before_total, position["buy_resource"], position["sell_resource"], amount)
        return True

    def _try_open_position(self, rates: Dict[str, float], summary: dict, before_total: int):
        fee_mul = 1 - FEE
        candidates = []
        for pair, rate in rates.items():
            res_a, res_b = pair.split("_")
            for sell, buy, net_rate in (
                (res_a, res_b, rate * fee_mul),
                (res_b, res_a, (1.0 / rate) * fee_mul),
            ):
                spread = net_rate - 1
                if spread <= OPEN_SPREAD_THRESHOLD:
                    continue

                balance = summary["balances"].get(sell, 0)
                if balance < MIN_TRADE_AMOUNT:
                    continue

                amount = int(balance * TRADE_RATIO)
                amount = min(balance, max(amount, MIN_TRADE_AMOUNT))
                if amount < MIN_TRADE_AMOUNT:
                    continue

                quote = self.executor.quote_swap(sell, buy, amount)
                if not quote:
                    continue

                expected_edge = quote["amount_out"] - quote["amount_in"]
                candidates.append({
                    "sell": sell,
                    "buy": buy,
                    "spread": spread * 100,
                    "amount_in": quote["amount_in"],
                    "amount_out": quote["amount_out"],
                    "expected_edge": expected_edge,
                })

        if not candidates:
            log.info(f"  当前没有总和收益率超过 {OPEN_SPREAD_THRESHOLD*100:.1f}% 的方向")
            return

        candidates.sort(key=lambda item: (item["expected_edge"], item["spread"]), reverse=True)
        best = candidates[0]
        log.info(
            f"  ★ 开仓 {best['sell']}→{best['buy']} | "
            f"总和收益率 {best['spread']:.2f}% | "
            f"记录价格 {best['amount_in']} {best['sell']} → {best['amount_out']} {best['buy']}"
        )

        result = self.executor.execute_swap(best["sell"], best["buy"], amount=best["amount_in"])
        if not result:
            log.info("  开仓失败，等待下一轮")
            return

        if not self.dry_run:
            actual_in = best["amount_in"]
            actual_out = best["amount_out"]
            try:
                time.sleep(1.0)
                after_state = self.planet_state_reader.read()
                before_sell = summary["balances"].get(best["sell"], 0)
                before_buy = summary["balances"].get(best["buy"], 0)
                after_sell = int(after_state["resources"].get(best["sell"], 0))
                after_buy = int(after_state["resources"].get(best["buy"], 0))
                if before_sell > after_sell:
                    actual_in = before_sell - after_sell
                if after_buy > before_buy:
                    actual_out = after_buy - before_buy
                log.info(f"  链上实际成交: {actual_in} {best['sell']} → {actual_out} {best['buy']}")
            except Exception as e:
                log.warning(f"  读取交易后余额失败，使用报价值: {e}")
            position = {
                "opened_at": datetime.now().isoformat(),
                "sell_resource": best["sell"],
                "buy_resource": best["buy"],
                "amount_in": actual_in,
                "amount_out": actual_out,
                "target_amount_back": calc_close_target_amount(actual_in),
                "entry_spread_pct": best["spread"],
                "open_tx": result,
            }
            self.positions.append(position)
            save_positions(self.positions)
            history = load_trade_history()
            history.append({
                **position,
                "status": "持仓中",
            })
            save_trade_history(history)
        self._log_realized_edge(before_total, best["sell"], best["buy"], best["amount_in"])

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
    pool_pubkey = None
    if PDA_FILE.exists():
        pdas = normalize_pdas(json.loads(PDA_FILE.read_text()))
        tp = pdas.get("trading_pools", pdas.get("pool_state", ""))
        if tp:
            pool_pubkey = Pubkey.from_string(tp)
    reader = PoolReader(rpc, pool_pubkey)
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

    # 先 Collect，把建筑累计产出结算到链上
    exe = SwapExecutor(rpc, kp, pdas, dry_run=False)
    print("\n正在执行 Collect（结算建筑产出）...")
    result = exe.execute_collect()
    if result:
        print(f"  Collect 成功: {result}")
        time.sleep(1.0)
    else:
        print("  Collect 失败或无需 Collect")

    state = PlanetStateReader(rpc, pdas).read()
    print("\n当前链上余额（Collect 后）:")
    for name in RESOURCE_BALANCE_NAMES:
        print(f"  {name}: {state['resources'][name]}")

    print(f"\n附加信息:")
    print(f"  season_id: {state['season_id']}")
    print(f"  leaderboard_points: {state['leaderboard_points']}")
    print(f"  planet_type: {state['planet_type']}")
    print(f"  energy: {state['energy']}")
    print(f"  stardust_exp: {state['stardust_exp']}")
    print(f"  planet_mint: {state['planet_mint']}")


def cmd_positions():
    positions = load_positions()
    if not positions:
        print("\n当前没有未平仓记录。")
        return

    do_check = "--check" in sys.argv
    rpc = Client(RPC_URL) if do_check else None
    kp = load_keypair() if do_check else None
    pdas = normalize_pdas(json.loads(PDA_FILE.read_text())) if do_check and PDA_FILE.exists() else None
    exe = SwapExecutor(rpc, kp, pdas, dry_run=True) if do_check and rpc and kp and pdas else None

    print(f"\n当前未平仓记录: {len(positions)}")
    for idx, pos in enumerate(positions, 1):
        opened_at = pos.get("opened_at", "unknown")
        sell = pos.get("sell_resource", "?")
        buy = pos.get("buy_resource", "?")
        amount_in = pos.get("amount_in", 0)
        amount_out = pos.get("amount_out", 0)
        target_back = calc_close_target_amount(int(amount_in))
        spread = pos.get("entry_spread_pct", 0.0)
        tx = pos.get("open_tx", "")
        print(f"\n[{idx}] {sell} → {buy}")
        print(f"  开仓时间: {opened_at}")
        print(f"  记录价格: {amount_in} {sell} → {amount_out} {buy}")
        print(f"  平仓目标: {amount_out} {buy} → 至少 {target_back} {sell} (+{CLOSE_PROFIT_TARGET*100:.1f}%)")
        print(f"  开仓总和收益率: {spread:.2f}%")
        if tx:
            print(f"  开仓交易: {tx}")
        if exe:
            quote = exe.quote_swap(buy, sell, int(amount_out))
            if quote:
                gap = quote["amount_out"] - target_back
                status = "已满足平仓条件" if gap >= 0 else f"还差 {-gap} {sell}"
                print(f"  当前检查: {amount_out} {buy} → {quote['amount_out']} {sell}，{status}")
            else:
                print("  当前检查: 无法获取反向报价")


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
    elif cmd == "analyze-mine":
        cmd_analyze_mine()
    elif cmd == "positions":
        cmd_positions()
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
        print(f"未知命令: {cmd}\n可用: discover | analyze-mine [--pages N] [--page-size N] [--crit-threshold N] | positions [--check] | balances | rates | verify | monitor [--live]")


if __name__ == "__main__":
    main()
