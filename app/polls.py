"""现场决策投票 —— 存储与结算域（不依赖 fastapi，便于无第三方依赖测试）。

与 app.main 同一把 state_lock、同一个 SQLite 连接下被调用，因此本模块
不再自行加锁，只负责：

- 创建投票（2～4 个选项 + 指定参与组白名单 + 截止虚拟毫秒，归属当前轮次）
- 组投票 / 改投（UNIQUE(poll_id, group_id) 保证每组只计一票，改投原地更新）
- tick 结算：对「当前轮次、open、deadline_ms <= 虚拟时钟」的投票一次性
  结算——最高票获胜，平票按选项创建顺序（seq 最小）决定；获胜项、结算虚拟
  毫秒与**各选项票数**在同一事务写回 polls/poll_options 行（tallies_state='
  frozen'）；之后删除参与组只会级联删除其残余选票，读取一律取固定值，
  刷新/重启/删组都展示结算当时的结果（option_tallies 是主持人/参与者
  视图共用的唯一票数口径）；
  结算只数本轮票，且每条投票只结算一次（status open → settled）
- 旧库恢复：已结算但 tallies_state IS NULL 的行由 recover_settled_tallies
  从对应 poll_settled 事件恢复票数；事件缺失置 'unrecoverable'——保留
  获胜项、明确标示票数不可恢复，绝不用残余选票伪造
- 重置：未结算（open）投票作废（status → voided、票删除），已结算结果
  按轮次保留不动（含固定票数）

暂停冻结截止计时由调用方保证：tick 只扫描 running 房间并以
main.room_elapsed_ms（暂停取冻结值）作为当前虚拟毫秒；重启时 init_schema
把 running 房间安全冻结为 paused，到期扫描自然暂停，状态全部由持久化行推导。
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

MIN_OPTIONS = 2
MAX_OPTIONS = 4
MAX_DEADLINE_SECONDS = 24 * 3600

# polls.tallies_state 取值
TALLIES_FROZEN = "frozen"                # 票数已在结算当时固定（或从结算事件恢复）
TALLIES_UNRECOVERABLE = "unrecoverable"  # 旧库已结算行缺结算事件：保留获胜项，票数不可恢复


class PollError(Exception):
    """投票域校验错误。main.py 捕获后转成对应 HTTP 状态码（detail 为消息文本）。"""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# ---------------------------------------------------------------- 创建

def create_poll(
    conn: sqlite3.Connection,
    room_id: int,
    question: str,
    options: list[str],
    group_ids: list[int],
    deadline_ms: int,
    round_idx: int,
    *,
    now: float | None = None,
) -> sqlite3.Row:
    """创建一条 open 投票并写入选项、参与组白名单。调用方须先完成输入校验
    （选项 2～4 个非空、参与组属于本房间、截止虚拟毫秒晚于当前虚拟时钟）。"""
    now = time.time() if now is None else now
    cur = conn.execute(
        "INSERT INTO polls (room_id, question, round, deadline_ms, status, created_at)"
        " VALUES (?, ?, ?, ?, 'open', ?)",
        (room_id, question, round_idx, deadline_ms, now),
    )
    poll_id = cur.lastrowid
    for seq, text in enumerate(options):
        conn.execute(
            "INSERT INTO poll_options (poll_id, seq, text) VALUES (?, ?, ?)",
            (poll_id, seq, text),
        )
    for gid in dict.fromkeys(group_ids):  # 去重保序
        conn.execute(
            "INSERT INTO poll_groups (poll_id, group_id) VALUES (?, ?)",
            (poll_id, gid),
        )
    conn.commit()
    return conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()


def get_poll(conn: sqlite3.Connection, room_id: int, poll_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM polls WHERE id = ? AND room_id = ?", (poll_id, room_id)
    ).fetchone()


def poll_group_ids(conn: sqlite3.Connection, poll_id: int) -> list[int]:
    return [r["group_id"] for r in conn.execute(
        "SELECT group_id FROM poll_groups WHERE poll_id = ? ORDER BY group_id", (poll_id,)
    )]


def poll_options(conn: sqlite3.Connection, poll_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM poll_options WHERE poll_id = ? ORDER BY seq", (poll_id,)
    ).fetchall()


# ---------------------------------------------------------------- 投票 / 改投

@dataclass
class VoteResult:
    poll_id: int
    group_id: int
    option_id: int
    first_vote: bool   # False = 改投（原地更新既有票）


def cast_vote(
    conn: sqlite3.Connection,
    poll: sqlite3.Row,
    group_id: int,
    option_id: int,
    *,
    now: float | None = None,
) -> VoteResult:
    """指定组提交或改投。调用方须先验证：组在白名单内、投票 open、
    选项属于本投票；截止判定（虚拟时钟）也由调用方做，错误统一抛 PollError。"""
    now = time.time() if now is None else now
    existing = conn.execute(
        "SELECT id FROM poll_votes WHERE poll_id = ? AND group_id = ?",
        (poll["id"], group_id),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO poll_votes (room_id, poll_id, group_id, option_id, round, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (poll["room_id"], poll["id"], group_id, option_id, poll["round"], now),
        )
        first_vote = True
    else:
        conn.execute(
            "UPDATE poll_votes SET option_id = ?, round = ?, updated_at = ? WHERE id = ?",
            (option_id, poll["round"], now, existing["id"]),
        )
        first_vote = False
    conn.commit()
    return VoteResult(poll_id=poll["id"], group_id=group_id,
                      option_id=option_id, first_vote=first_vote)


# ---------------------------------------------------------------- tick 结算

@dataclass
class SettleResult:
    poll: sqlite3.Row
    winner: sqlite3.Row
    tallies: dict[int, int]   # option_id -> 票数（未得票选项为 0）


def settle_due_polls(
    conn: sqlite3.Connection,
    room_id: int,
    round_idx: int,
    elapsed_ms: int,
) -> list[SettleResult]:
    """在锁内对当前轮次所有到期且仍 open 的投票一次性结算。

    每条投票只可能被结算一次：先按 status='open' 取出、写回 settled 并
    commit，后续 tick（以及重启后）不会再次结算。最高票获胜；平票按
    选项创建顺序（seq 最小）决定——全员零票时选项 0 获胜，确定性、可复现。
    """
    due = conn.execute(
        """SELECT * FROM polls
           WHERE room_id = ? AND round = ? AND status = 'open' AND deadline_ms <= ?
           ORDER BY deadline_ms, id""",
        (room_id, round_idx, elapsed_ms),
    ).fetchall()
    results: list[SettleResult] = []
    for poll in due:
        options = conn.execute(
            "SELECT * FROM poll_options WHERE poll_id = ? ORDER BY seq", (poll["id"],)
        ).fetchall()
        tallies = {opt["id"]: 0 for opt in options}
        for row in conn.execute(
            "SELECT option_id, COUNT(*) AS c FROM poll_votes"
            " WHERE poll_id = ? AND round = ? GROUP BY option_id",
            (poll["id"], round_idx),
        ):
            if row["option_id"] in tallies:  # 防御：孤立 option_id 不计入
                tallies[row["option_id"]] = row["c"]
        # max 在票数相等时保留先遇到者；options 已按 seq 升序 → 平票取创建最早选项
        winner = max(options, key=lambda opt: (tallies[opt["id"]], -opt["seq"]))
        # 结果固定化：状态、获胜项、各选项票数在同一事务写死。此后参与组被删
        # 只会级联删除其选票，读取一律取固定值，绝不按残余选票重算（否则会出现
        # 获胜项 0 票、他项 1 票的矛盾展示）。每条投票只在 open→settled 时写一次。
        for opt in options:
            conn.execute(
                "UPDATE poll_options SET settled_votes = ? WHERE id = ?",
                (tallies[opt["id"]], opt["id"]),
            )
        conn.execute(
            "UPDATE polls SET status = 'settled', winner_option_id = ?, settled_at_ms = ?,"
            " tallies_state = ? WHERE id = ? AND status = 'open'",
            (winner["id"], elapsed_ms, TALLIES_FROZEN, poll["id"]),
        )
        conn.commit()
        fresh = conn.execute("SELECT * FROM polls WHERE id = ?", (poll["id"],)).fetchone()
        results.append(SettleResult(poll=fresh, winner=winner, tallies=tallies))
    return results


# ---------------------------------------------------------------- 重置

def void_open_polls(conn: sqlite3.Connection, room_id: int) -> list[int]:
    """重置：作废当前所有未结算（open）投票并删除其选票；已结算结果保留。

    返回被作废的投票 id 列表（调用方据此逐条广播 poll_voided，仅主持人可见）。
    选项与参与组白名单随投票行保留（复盘作废投票的元数据仍可查）。
    """
    open_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM polls WHERE room_id = ? AND status = 'open' ORDER BY id",
        (room_id,),
    ).fetchall()]
    for poll_id in open_ids:
        conn.execute("DELETE FROM poll_votes WHERE poll_id = ?", (poll_id,))
        conn.execute(
            "UPDATE polls SET status = 'voided' WHERE id = ? AND status = 'open'",
            (poll_id,),
        )
    conn.commit()
    return open_ids


# ---------------------------------------------------------------- 读取

def vote_of_group(conn: sqlite3.Connection, poll_id: int, group_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM poll_votes WHERE poll_id = ? AND group_id = ?",
        (poll_id, group_id),
    ).fetchone()


def option_tallies(
    conn: sqlite3.Connection, poll: sqlite3.Row
) -> tuple[list[sqlite3.Row], dict[int, int | None], str | None]:
    """读取一条投票的选项与各选项票数，主持人/参与者视图共用此唯一口径。

    返回 (options, tallies, state)：

    - 已结算且 tallies_state='frozen'：取 poll_options.settled_votes——结算当时
      固定的票数；之后选票因参与组删除被级联清理也不受影响（重启/刷新一致）；
    - 已结算且 tallies_state='unrecoverable'：旧库缺对应 poll_settled 事件，
      保留 winner_option_id 但每选项票数为 None（读取方按「票数不可恢复」标示），
      绝不按残余选票重算伪造；
    - 未结算（open/voided，state 为 None）：按当前残余 poll_votes 实时统计
      （voided 票已被重置删除，恒为 0）。
    """
    options = conn.execute(
        "SELECT * FROM poll_options WHERE poll_id = ? ORDER BY seq", (poll["id"],)
    ).fetchall()
    state = poll["tallies_state"] if "tallies_state" in poll.keys() else None
    if state == TALLIES_FROZEN:
        return options, {o["id"]: (0 if o["settled_votes"] is None else o["settled_votes"])
                         for o in options}, state
    if state == TALLIES_UNRECOVERABLE:
        return options, {o["id"]: None for o in options}, state
    tallies = {o["id"]: 0 for o in options}
    for row in conn.execute(
        "SELECT option_id, COUNT(*) AS c FROM poll_votes WHERE poll_id = ? GROUP BY option_id",
        (poll["id"],),
    ):
        if row["option_id"] in tallies:
            tallies[row["option_id"]] = row["c"]
    return options, tallies, None


def recover_settled_tallies(conn: sqlite3.Connection) -> dict[int, str]:
    """旧库升级恢复：为 tallies_state IS NULL 的已结算投票固定票数。

    从该房间对应 poll_settled 事件（payload 即结算当时的主持人全量视图，
    含 options[].id 与 tallies[{option_id,count}]）逐条恢复，写入
    poll_options.settled_votes 并置 tallies_state='frozen'；事件缺失或载荷
    损坏（缺某选项计数）时置 'unrecoverable'：winner_option_id 原样保留，
    settled_votes 留 NULL，读取端明确标示票数不可恢复。

    幂等：只挑 tallies_state IS NULL 的已结算行处理；新版本结算时必写
    tallies_state，因此不会重复执行。返回 {poll_id: 恢复后的 state}。
    """
    pending = conn.execute(
        "SELECT * FROM polls WHERE status = 'settled' AND tallies_state IS NULL"
    ).fetchall()
    recovered: dict[int, str] = {}
    for poll in pending:
        state = TALLIES_UNRECOVERABLE
        event = conn.execute(
            "SELECT payload FROM events WHERE room_id = ? AND type = 'poll_settled'"
            " AND json_extract(payload, '$.id') = ? ORDER BY id LIMIT 1",
            (poll["room_id"], poll["id"]),
        ).fetchone()
        if event is not None:
            try:
                payload = json.loads(event["payload"])
                counts = {int(t["option_id"]): int(t["count"])
                          for t in payload.get("tallies", [])}
                options = conn.execute(
                    "SELECT id FROM poll_options WHERE poll_id = ?", (poll["id"],)
                ).fetchall()
                if options and all(o["id"] in counts for o in options):
                    for o in options:
                        conn.execute(
                            "UPDATE poll_options SET settled_votes = ? WHERE id = ?",
                            (counts[o["id"]], o["id"]),
                        )
                    state = TALLIES_FROZEN
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                state = TALLIES_UNRECOVERABLE
        conn.execute("UPDATE polls SET tallies_state = ? WHERE id = ?", (state, poll["id"]))
        recovered[poll["id"]] = state
    conn.commit()
    return recovered
