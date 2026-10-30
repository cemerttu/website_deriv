"""
Deriv Market Analyzer — Over 1 / Under 8
REAL market data only. No random/demo fallback and no trade placement.

Requirements:
    python -m pip install websocket-client

Run:
    python deriv_analyzer.py

Notes:
- Uses Deriv's public market-data WebSocket; no account token is needed.
- This program NEVER buys or places trades.
- Backtest P/L is an estimate using the configurable payout assumption below.
- Market symbols are checked against Deriv's active-symbol list at runtime.
"""

import json
import math
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

try:
    from websocket import create_connection, WebSocketTimeoutException
except ImportError:
    print("Missing dependency: websocket-client")
    print("Install it with: python -m pip install websocket-client")
    sys.exit(1)


# ============================================================
# SETTINGS
# ============================================================

# Current public Deriv API WebSocket endpoint (market data; no login needed).
WS_ENDPOINT = "wss://api.derivws.com/trading/v1/options/ws/public"

# Keep the original classic volatility markets. The program will check which
# of these are actually available from Deriv's active_symbols response.
MARKETS = [
    ("R_10", "Volatility 10"),
    ("R_25", "Volatility 25"),
    ("R_50", "Volatility 50"),
    ("R_75", "Volatility 75"),
    ("R_100", "Volatility 100"),
]

TICK_COUNT = 3000
LIVE_SECONDS = 300
STAKE = 1.0

# Estimated P/L assumption only. It is NOT a live quote from Deriv.
# Example: 0.80 means a winning $1 stake returns $1.80 total, i.e. $0.80 profit.
ASSUMED_PROFIT_PER_WIN = 0.80

CONNECT_TIMEOUT = 15
RECEIVE_TIMEOUT = 2
MAX_RETRIES = 3

# Strategy definitions from the original program:
# Over 1 wins on last digit 2..9; Under 8 wins on last digit 0..7.
def over_wins(digit):
    return digit >= 2

def under_wins(digit):
    return digit <= 7


def estimated_pl(wins, losses):
    return wins * ASSUMED_PROFIT_PER_WIN - losses * STAKE


def timestamp_from_epoch(epoch):
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).astimezone().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except (TypeError, ValueError, OSError):
        return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# DERIV PUBLIC WEBSOCKET CLIENT
# ============================================================

class DerivClient:
    def __init__(self):
        self.ws = None
        self.request_id = 0
        self.decimals = {}
        self.active_symbols = []
        self.symbol_names = {}
        self.tick_subscriptions = {}

    def connect(self):
        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"Connecting to Deriv public market data (attempt {attempt}/{MAX_RETRIES})...")
                self.ws = create_connection(
                    WS_ENDPOINT,
                    timeout=CONNECT_TIMEOUT,
                    enable_multithread=True,
                )
                self.ws.settimeout(RECEIVE_TIMEOUT)
                # Verify the connection can respond.
                response = self.request({"ping": 1}, expected_types={"ping"})
                if response:
                    print("Connected to Deriv. Public market-data connection verified.\n")
                    return
                raise ConnectionError("No valid ping response received.")
            except Exception as exc:
                last_error = exc
                self.close()
                if attempt < MAX_RETRIES:
                    time.sleep(attempt * 2)
        raise ConnectionError(
            f"Could not connect to Deriv after {MAX_RETRIES} attempts. "
            f"Check internet/firewall and try again. Last error: {last_error}"
        )

    def _send(self, payload):
        if self.ws is None:
            raise ConnectionError("Not connected to Deriv.")
        self.request_id += 1
        payload = dict(payload)
        payload["req_id"] = self.request_id
        self.ws.send(json.dumps(payload))
        return self.request_id

    def _recv_json(self, timeout=None):
        if self.ws is None:
            raise ConnectionError("Not connected to Deriv.")
        old_timeout = None
        try:
            if timeout is not None:
                old_timeout = self.ws.gettimeout()
                self.ws.settimeout(timeout)
            raw = self.ws.recv()
            if not raw:
                raise ConnectionError("Deriv closed the WebSocket connection.")
            message = json.loads(raw)
            if "error" in message:
                error = message["error"]
                raise RuntimeError(error.get("message", str(error)))
            return message
        finally:
            if timeout is not None and self.ws is not None:
                try:
                    self.ws.settimeout(old_timeout)
                except Exception:
                    pass

    def request(self, payload, expected_types=None, timeout=CONNECT_TIMEOUT):
        """Send one request and wait for its matching response, ignoring unrelated messages."""
        req_id = self._send(payload)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                message = self._recv_json(timeout=max(0.2, min(2, deadline - time.time())))
            except WebSocketTimeoutException:
                continue
            if message.get("req_id") != req_id:
                # For this simple CLI, no other request should be in flight during setup.
                continue
            if expected_types and message.get("msg_type") not in expected_types:
                continue
            return message
        raise TimeoutError(f"Timed out waiting for Deriv response to {payload!r}")

    def load_active_symbols(self):
        response = self.request(
            {"active_symbols": "brief"},
            expected_types={"active_symbols"},
        )
        self.active_symbols = response.get("active_symbols", [])
        if not self.active_symbols:
            raise RuntimeError(
                "Deriv returned no active symbols. The API response did not contain markets."
            )

        # Index by symbol and by normalized display name.
        self.symbol_names = {}
        for item in self.active_symbols:
            symbol = item.get("symbol") or item.get("underlying_symbol")
            if not symbol:
                continue
            display_name = (
                item.get("display_name")
                or item.get("underlying_symbol_name")
                or symbol
            )
            self.symbol_names[symbol] = display_name

            pip_size = item.get("pip")
            if pip_size is None:
                pip_size = item.get("pip_size")
            if pip_size is not None:
                try:
                    pip_value = float(pip_size)
                    decimals = (
                        round(-math.log10(pip_value))
                        if 0 < pip_value < 1
                        else int(pip_value)
                    )
                    self.decimals[symbol] = max(0, min(10, decimals))
                except (ValueError, TypeError):
                    pass

        return self.resolve_markets()

    def resolve_markets(self):
        """Use configured symbols when present; otherwise match display names carefully."""
        available = {
            item.get("symbol") or item.get("underlying_symbol"): item
            for item in self.active_symbols
            if item.get("symbol") or item.get("underlying_symbol")
        }
        resolved = []
        missing = []

        for preferred_symbol, label in MARKETS:
            if preferred_symbol in available:
                item = available[preferred_symbol]
                display_name = (
                    item.get("display_name")
                    or item.get("underlying_symbol_name")
                    or label
                )
                resolved.append((preferred_symbol, display_name))
                continue

            # Some Deriv symbol lists use a display name that varies slightly.
            target_number = label.lower().replace("volatility", "").strip()
            match = None
            for item in self.active_symbols:
                name = str(
                    item.get("display_name")
                    or item.get("underlying_symbol_name")
                    or ""
                ).lower()
                symbol = str(
                    item.get("symbol") or item.get("underlying_symbol") or ""
                ).upper()
                if target_number in name and "volatility" in name:
                    match = item
                    break
                # Classic synthetic symbol aliases.
                if symbol == preferred_symbol.upper():
                    match = item
                    break

            if match and (match.get("symbol") or match.get("underlying_symbol")):
                symbol = match.get("symbol") or match.get("underlying_symbol")
                display_name = (
                    match.get("display_name")
                    or match.get("underlying_symbol_name")
                    or label
                )
                resolved.append((symbol, display_name))
            else:
                missing.append(label)

        if missing:
            print("Note: These requested markets were not found in Deriv's active-symbol list:")
            for label in missing:
                print(f"  - {label}")
            print("Only markets confirmed as available will be used.\n")

        if not resolved:
            examples = [
                f"{item.get('display_name') or item.get('underlying_symbol_name')} "
                f"[{item.get('symbol') or item.get('underlying_symbol')}]"
                for item in self.active_symbols
                if item.get("symbol") or item.get("underlying_symbol")
            ][:20]
            raise RuntimeError(
                "None of the configured volatility markets were found. "
                "Check the current Deriv symbol names. Sample available markets: "
                + ", ".join(examples)
            )
        return resolved

    def decimals_for(self, symbol, quote=None, pip_size=None):
        if pip_size is not None:
            try:
                return max(0, min(10, int(pip_size)))
            except (TypeError, ValueError):
                pass
        if symbol in self.decimals:
            return self.decimals[symbol]
        # Fallback: infer decimal places from string representation, if possible.
        if quote is not None:
            text = str(quote)
            if "." in text:
                return min(10, len(text.split(".", 1)[1]))
        return 2

    def last_digit(self, quote, symbol, pip_size=None):
        decimals = self.decimals_for(symbol, quote, pip_size)
        formatted = f"{float(quote):.{decimals}f}"
        digits = "".join(ch for ch in formatted if ch.isdigit())
        if not digits:
            raise ValueError(f"Could not extract last digit from quote {quote!r}")
        return int(digits[-1])

    def history(self, symbol, count):
        response = self.request(
            {
                "ticks_history": symbol,
                "count": int(count),
                "end": "latest",
                "style": "ticks",
            },
            expected_types={"history"},
            timeout=CONNECT_TIMEOUT,
        )
        history = response.get("history", {})
        prices = history.get("prices", [])
        times = history.get("times", [])
        return prices, times

    def subscribe_ticks(self, symbol):
        req_id = self._send({"ticks": symbol, "subscribe": 1})
        # Wait for the first response for this subscription. It is usually a tick.
        deadline = time.time() + CONNECT_TIMEOUT
        while time.time() < deadline:
            try:
                message = self._recv_json(timeout=2)
            except WebSocketTimeoutException:
                continue
            if message.get("req_id") == req_id and message.get("msg_type") == "tick":
                tick = message.get("tick", {})
                sub = message.get("subscription", {})
                if sub.get("id"):
                    self.tick_subscriptions[symbol] = sub["id"]
                return tick
            if message.get("req_id") == req_id and message.get("msg_type") == "error":
                raise RuntimeError(str(message.get("error")))
        raise TimeoutError(f"No initial tick received for {symbol}.")

    def next_tick(self):
        """Read the next tick message; ignore non-tick messages."""
        while True:
            try:
                message = self._recv_json(timeout=RECEIVE_TIMEOUT)
            except WebSocketTimeoutException:
                return None
            if message.get("msg_type") != "tick":
                continue
            tick = message.get("tick", {})
            symbol = tick.get("symbol")
            quote = tick.get("quote")
            if symbol is None or quote is None:
                continue
            return {
                "symbol": symbol,
                "quote": float(quote),
                "epoch": tick.get("epoch"),
                "pip_size": tick.get("pip_size"),
            }

    def close(self):
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None


# ============================================================
# BOT XML LOADER
# ============================================================

def load_bot_xml():
    path = input('Paste or drag your Deriv Bot XML file path here: ').strip().strip('"')
    if not path:
        print("No file path entered.\n")
        return
    if not os.path.isfile(path):
        print(f"File not found: {path}\n")
        return

    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError) as exc:
        print(f"Could not read a valid XML file: {exc}\n")
        return

    fields = {}
    for element in root.iter():
        if element.tag.split("}")[-1] == "field":
            name = element.get("name")
            if name and element.text:
                fields[name] = element.text.strip()

    def pick(*keys):
        for key in keys:
            if fields.get(key):
                return fields[key]
        return "-"

    blocks = sum(1 for element in root.iter() if element.tag.split("}")[-1] == "block")
    print("\n----- Bot XML Summary (best-effort field scan) -----")
    print(f"  Market             : {pick('SYMBOL_LIST', 'MARKET_LIST', 'SUBMARKET_LIST')}")
    print(f"  Trade type         : {pick('TRADETYPE_LIST', 'TRADETYPECAT_LIST')}")
    print(f"  Contract            : {pick('TYPE_LIST', 'CONTRACT_TYPE')}")
    print(f"  Prediction/barrier : {pick('PREDICTION', 'BARRIER', 'LASTDIGITPREDICTION')}")
    print(f"  Stake               : {pick('AMOUNT')}")
    print(f"  Duration            : {pick('DURATION')} {pick('DURATIONTYPE_LIST')}")
    print(f"  Currency            : {pick('CURRENCY_LIST')}")
    print(f"  Total blocks        : {blocks}")
    print("Note: this is a simple XML field scan, not a full execution of the bot logic.")
    print("---------------------------------------------------\n")


# ============================================================
# HISTORICAL BACKTEST
# ============================================================

def run_historical(api, markets):
    print(
        f"\nFetching up to {TICK_COUNT} REAL historical ticks per market...\n"
        "Estimated P/L uses your configured payout assumption, not a live proposal.\n"
    )
    rows = []

    for symbol, name in markets:
        try:
            prices, times = api.history(symbol, TICK_COUNT)
            if not prices:
                print(f"  {name}: no historical prices returned.")
                continue

            over_count = 0
            under_count = 0
            for price in prices:
                digit = api.last_digit(price, symbol)
                over_count += int(over_wins(digit))
                under_count += int(under_wins(digit))

            total = len(prices)
            over_losses = total - over_count
            under_losses = total - under_count
            over_pl = estimated_pl(over_count, over_losses)
            under_pl = estimated_pl(under_count, under_losses)

            rows.append({
                "symbol": symbol,
                "name": name,
                "total": total,
                "over_rate": over_count / total * 100,
                "under_rate": under_count / total * 100,
                "over_pl": over_pl,
                "under_pl": under_pl,
            })
        except Exception as exc:
            print(f"  {name}: failed to fetch/analyse history: {exc}")

    if not rows:
        print("\nNo historical results. Check the connection and market availability.\n")
        return

    header = (
        f"{'Market':<25}{'Ticks':>8}{'Over1 Win%':>13}{'Over1 est.P/L':>16}"
        f"{'Under8 Win%':>14}{'Under8 est.P/L':>17}{'Better est.':>13}"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        better = "Over 1" if row["over_pl"] >= row["under_pl"] else "Under 8"
        print(
            f"{row['name'][:24]:<25}{row['total']:>8}"
            f"{row['over_rate']:>12.2f}%{row['over_pl']:>15.2f}"
            f"{row['under_rate']:>13.2f}%{row['under_pl']:>16.2f}{better:>13}"
        )

    scored = []
    for row in rows:
        if row["over_pl"] >= row["under_pl"]:
            scored.append((row["name"], "Over 1", row["over_pl"]))
        else:
            scored.append((row["name"], "Under 8", row["under_pl"]))
    scored.sort(key=lambda item: item[2], reverse=True)

    best = scored[0]
    worst = scored[-1]
    print(f"\nBEST market (by estimated P/L):  {best[0]} — {best[1]}, {best[2]:+.2f}")
    print(f"WORST market (by estimated P/L): {worst[0]} — {worst[1]}, {worst[2]:+.2f}")
    print(
        "\nImportant: this is a historical digit-frequency test, not proof of future profit. "
        "Over 1 and Under 8 overlap on digits 2–7, so their win rates are not complements.\n"
    )


# ============================================================
# LIVE ANALYSIS
# ============================================================

def run_live(api, markets):
    print(f"\nLIVE ANALYSIS for {LIVE_SECONDS} seconds.")
    print("Source: Deriv public WebSocket. No trades will be placed.")
    print("A received tick is printed with its local timestamp and actual quote.\n")

    market_names = {symbol: name for symbol, name in markets}
    stats = {
        symbol: {"ticks": 0, "over": 0, "under": 0, "last_quote": None, "last_time": None}
        for symbol, _ in markets
    }

    # Subscribe one by one. Each successful initial response verifies a tick.
    subscribed = []
    for symbol, name in markets:
        try:
            first = api.subscribe_ticks(symbol)
            subscribed.append(symbol)
            quote = first.get("quote")
            epoch = first.get("epoch")
            pip_size = first.get("pip_size")
            digit = api.last_digit(quote, symbol, pip_size)
            stat = stats[symbol]
            stat["ticks"] += 1
            stat["over"] += int(over_wins(digit))
            stat["under"] += int(under_wins(digit))
            stat["last_quote"] = quote
            stat["last_time"] = timestamp_from_epoch(epoch)
            decimals = api.decimals_for(symbol, quote, pip_size)
            print(f"[LIVE] {stat['last_time']} | {name} [{symbol}] | "
                  f"price={quote:.{decimals}f} | last digit={digit}")
        except Exception as exc:
            print(f"Could not subscribe to {name} [{symbol}]: {exc}")

    if not subscribed:
        print("\nNo live tick subscriptions succeeded. No simulated data was used.\n")
        return

    end_time = time.time() + LIVE_SECONDS
    try:
        while time.time() < end_time:
            tick = api.next_tick()
            if tick is None:
                continue

            symbol = tick["symbol"]
            if symbol not in stats:
                continue

            quote = tick["quote"]
            epoch = tick.get("epoch")
            digit = api.last_digit(quote, symbol, tick.get("pip_size"))
            stat = stats[symbol]
            stat["ticks"] += 1
            stat["over"] += int(over_wins(digit))
            stat["under"] += int(under_wins(digit))
            stat["last_quote"] = quote
            stat["last_time"] = timestamp_from_epoch(epoch)

            decimals = api.decimals_for(symbol, quote, tick.get("pip_size"))
            print(f"[LIVE] {stat['last_time']} | {market_names[symbol]} [{symbol}] | "
                  f"price={quote:.{decimals}f} | last digit={digit}")

    except KeyboardInterrupt:
        print("\nLive analysis stopped by user.")
    except Exception as exc:
        print(f"\nLive stream stopped because of a connection/data error: {exc}")
        print("Results below include only real ticks received before the interruption.")

    print("\nLIVE SUMMARY")
    header = f"{'Market':<25}{'Ticks':>9}{'Over1 Win%':>14}{'Under8 Win%':>15}{'Latest price':>18}"
    print(header)
    print("-" * len(header))
    for symbol, name in markets:
        stat = stats[symbol]
        count = stat["ticks"]
        over_pct = stat["over"] / count * 100 if count else 0.0
        under_pct = stat["under"] / count * 100 if count else 0.0
        quote = stat["last_quote"]
        if quote is None:
            quote_text = "no tick"
        else:
            quote_text = f"{quote:.{api.decimals_for(symbol, quote)}f}"
        print(f"{name[:24]:<25}{count:>9}{over_pct:>13.2f}%{under_pct:>14.2f}%{quote_text:>18}")
    print("\nThese percentages describe observed tick digits during this session; they do not guarantee profit.\n")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 62)
    print("DERIV MARKET ANALYZER — OVER 1 / UNDER 8")
    print("REAL DATA ONLY | NO RANDOM FALLBACK | NO TRADES")
    print("=" * 62)

    api = DerivClient()
    try:
        api.connect()
        markets = api.load_active_symbols()
        print("Markets confirmed by Deriv:")
        for symbol, name in markets:
            print(f"  - {name} [{symbol}]")
        print()

        while True:
            print("Menu:")
            print("  1) Load / inspect Deriv Bot XML file")
            print("  2) Historical tick backtest")
            print("  3) Live tick analysis")
            print("  4) Quit")
            choice = input("Choose 1-4: ").strip()

            if choice == "1":
                load_bot_xml()
            elif choice == "2":
                run_historical(api, markets)
            elif choice == "3":
                run_live(api, markets)
            elif choice == "4":
                break
            else:
                print("Please enter 1, 2, 3, or 4.\n")

    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as exc:
        print(f"\nERROR: {exc}")
        print("No demo/random prices were substituted.")
    finally:
        api.close()

    print("Disconnected. No trades were placed.")


if __name__ == "__main__":
    main()
