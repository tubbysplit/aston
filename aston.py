import argparse
import asyncio
import json
import logging
import random
import sqlite3
import time
import urllib.request

from jito_searcher_client import get_async_searcher_client
from jito_searcher_client.convert import versioned_tx_to_protobuf_packet
from jito_searcher_client.generated.bundle_pb2 import Bundle
from jito_searcher_client.generated.searcher_pb2 import (
    GetTipAccountsRequest,
    SendBundleRequest,
    SubscribeBundleResultsRequest,
)
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

log = logging.getLogger("recorder")

SCHEMA = """
CREATE TABLE IF NOT EXISTS bundle_results (
    ts        REAL NOT NULL,
    bundle_id TEXT NOT NULL,
    kind      TEXT NOT NULL,
    slot      INTEGER,
    detail    TEXT
);
CREATE TABLE IF NOT EXISTS sent (
    ts          REAL NOT NULL,
    uuid        TEXT PRIMARY KEY,
    tip_account TEXT NOT NULL,
    lamports    INTEGER NOT NULL
);
"""

REPORT_SQL = """
SELECT COUNT(*), COALESCE(SUM(landed), 0) FROM (
    SELECT s.uuid, MAX(r.kind IN ('processed', 'finalized')) AS landed
    FROM sent s LEFT JOIN bundle_results r ON r.bundle_id = s.uuid
    GROUP BY s.uuid
)
"""


def load_keypair(path):
    with open(path) as f:
        return Keypair.from_bytes(bytes(json.load(f)))


def describe(result, kind):
    if not kind:
        return None, ""
    sub = getattr(result, kind)
    slot = getattr(sub, "slot", None)
    detail = " ".join(str(sub).split())
    return slot, detail


def latest_blockhash(rpc_url):
    params = [{"commitment": "finalized"}]
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": params}).encode()
    req = urllib.request.Request(rpc_url, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return Hash.from_string(json.load(resp)["result"]["value"]["blockhash"])


def build_tip_tx(keypair, tip_account, lamports, blockhash):
    ix = transfer(TransferParams(from_pubkey=keypair.pubkey(),
                                 to_pubkey=Pubkey.from_string(tip_account), lamports=lamports))
    message = Message.new_with_blockhash([ix], keypair.pubkey(), blockhash)
    return VersionedTransaction(message, [keypair])


async def record(client, conn, commit_every):
    stream = client.SubscribeBundleResults(SubscribeBundleResultsRequest())
    pending = 0
    async for result in stream:
        kind = result.WhichOneof("result") or ""
        slot, detail = describe(result, kind)
        conn.execute(
            "INSERT INTO bundle_results (ts, bundle_id, kind, slot, detail) "
            "VALUES (?, ?, ?, ?, ?)",
            (time.time(), result.bundle_id, kind, slot, detail),
        )
        pending += 1
        log.info("bundle %s: %s (slot=%s)", result.bundle_id, kind, slot)
        if pending >= commit_every:
            conn.commit()
            pending = 0


async def send_bundles(client, conn, keypair, rpc_url, count):
    tip_accounts = (await client.GetTipAccounts(GetTipAccountsRequest())).accounts
    blockhash = await asyncio.to_thread(latest_blockhash, rpc_url)
    for i in range(count):
        tip_account = random.choice(tip_accounts)
        lamports = 1000 + i
        tx = build_tip_tx(keypair, tip_account, lamports, blockhash)
        resp = await client.SendBundle(SendBundleRequest(bundle=Bundle(packets=[versioned_tx_to_protobuf_packet(tx)])))
        conn.execute("INSERT INTO sent (ts, uuid, tip_account, lamports) VALUES (?, ?, ?, ?)",
                     (time.time(), resp.uuid, tip_account, lamports))
        conn.commit()
        log.info("sent bundle %s (%d lamports to %s)", resp.uuid, lamports, tip_account)


def report(conn):
    sent, landed = conn.execute(REPORT_SQL).fetchone()
    rate = landed / sent if sent else 0.0
    log.info("own bundles: landed=%d sent=%d rate=%.1f%%", landed, sent, rate * 100)


async def run(args):
    conn = sqlite3.connect(args.db)
    conn.executescript(SCHEMA)
    if args.report:
        report(conn)
        conn.close()
        return
    try:
        keypair = load_keypair(args.keypair)
        client = await get_async_searcher_client(args.block_engine, keypair)
        log.info("recording bundle results from %s into %s", args.block_engine, args.db)
        recorder = asyncio.create_task(record(client, conn, args.commit_every))
        if args.send:
            await send_bundles(client, conn, keypair, args.rpc, args.send)
        await recorder
    finally:
        conn.commit()
        report(conn)
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="record jito bundle results into sqlite")
    parser.add_argument("--keypair", required=True, help="path to Solana keypair JSON")
    parser.add_argument("--block-engine", default="mainnet.block-engine.jito.wtf", help="block engine gRPC URL")
    parser.add_argument("--rpc", default="https://api.mainnet-beta.solana.com", help="JSON-RPC URL for blockhash")
    parser.add_argument("--db", default="bundle_results.sqlite", help="sqlite database path")
    parser.add_argument("--commit-every", type=int, default=100, help="commit after this many rows")
    parser.add_argument("--send", type=int, default=0, help="send N tip bundles before recording")
    parser.add_argument("--report", action="store_true", help="print landed/sent rate and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("interrupted, committed pending rows and closed database")


if __name__ == "__main__":
    main()
