import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


@dataclass
class Position:
    symbol: str
    quantity: float = 0.0
    avg_price: float = 0.0


@dataclass
class Portfolio:
    cash: float = 100_000.0
    positions: Dict[str, Position] = field(default_factory=dict)
    equity_curve: List[Dict[str, float]] = field(default_factory=list)
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    max_equity: float = 0.0

    def update_position(self, symbol: str, fill_qty: float, fill_price: float) -> None:
        """Update position after a fill. Positive qty = buy, negative = sell."""

        pos = self.positions.get(symbol, Position(symbol=symbol))

        # Realized P&L on closing existing position
        if fill_qty < 0 < pos.quantity:
            closed = min(pos.quantity, -fill_qty)
            self.realized_pnl += closed * (fill_price - pos.avg_price)

        new_qty = pos.quantity + fill_qty

        if new_qty == 0:
            pos.quantity = 0.0
            pos.avg_price = 0.0
        elif fill_qty > 0:
            # average price for buys only
            pos.avg_price = (
                pos.avg_price * pos.quantity + fill_qty * fill_price
            ) / new_qty
            pos.quantity = new_qty
        else:
            # sells reduce quantity at existing avg_price
            pos.quantity = new_qty

        self.positions[symbol] = pos

    def update_cash(self, fill_qty: float, fill_price: float, fees: float) -> None:
        self.cash -= fill_qty * fill_price + fees

    def _compute_volatility(self, window: int = 50) -> float:
        if len(self.equity_curve) < 2:
            return 0.0

        returns: List[float] = []
        recent = self.equity_curve[-window:]
        for prev, cur in zip(recent[:-1], recent[1:]):
            p0 = prev["equity"]
            p1 = cur["equity"]
            if p0 > 0:
                returns.append((p1 - p0) / p0)

        if not returns:
            return 0.0

        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(var)

    def mark_to_market(self, prices: Dict[str, float]) -> Dict:
        """Recalculate unrealized P&L and portfolio equity."""

        self.unrealized_pnl = 0.0
        equity = self.cash

        for symbol, pos in self.positions.items():
            if pos.quantity == 0:
                continue
            price = prices.get(symbol, pos.avg_price)
            self.unrealized_pnl += pos.quantity * (price - pos.avg_price)
            equity += pos.quantity * price

        self.max_equity = max(self.max_equity, equity)
        drawdown = (
            (self.max_equity - equity) / self.max_equity
            if self.max_equity > 0
            else 0.0
        )

        timestamp = datetime.utcnow().isoformat() + "Z"
        point = {"timestamp": timestamp, "equity": equity}
        self.equity_curve.append(point)

        volatility = self._compute_volatility()

        snapshot = {
            "timestamp": timestamp,
            "cash": self.cash,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "equity": equity,
            "drawdown": drawdown,
            "volatility": volatility,
            "positions": [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "avg_price": p.avg_price,
                }
                for p in self.positions.values()
                if p.quantity != 0
            ],
            "equity_curve": self.equity_curve[-200:],
        }
        return snapshot

    def to_json(self) -> str:
        return json.dumps(self.mark_to_market({}), indent=2)
