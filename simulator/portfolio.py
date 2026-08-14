import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple


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
    kill_switch: bool = False
    max_drawdown_limit: float = 0.20
    daily_loss_limit: float = 0.05
    max_position_value_per_symbol: float = 25_000.0
    max_total_position_value: float = 100_000.0
    daily_start_equity: float = 100_000.0
    last_risk_rejection: Optional[str] = None

    def set_kill_switch(self, enabled: bool) -> None:
        self.kill_switch = bool(enabled)

    def _current_equity(self, prices: Optional[Dict[str, float]] = None) -> float:
        price_map = prices or {}
        equity = self.cash
        for symbol, pos in self.positions.items():
            if pos.quantity == 0:
                continue
            price = price_map.get(symbol, pos.avg_price)
            equity += pos.quantity * price
        return equity

    def _position_value(self, symbol: str, price: float, qty: Optional[float] = None) -> float:
        pos = self.positions.get(symbol, Position(symbol=symbol))
        quantity = pos.quantity if qty is None else qty
        if quantity == 0:
            return 0.0
        return abs(quantity) * price

    def _projected_risk_state(
        self,
        symbol: str,
        fill_qty: float,
        fill_price: float,
        fees: float = 0.0,
        prices: Optional[Dict[str, float]] = None,
    ) -> Tuple[float, float, float]:
        price_map = prices or {}
        projected_positions: Dict[str, float] = {
            sym: pos.quantity for sym, pos in self.positions.items()
        }
        current_pos = self.positions.get(symbol, Position(symbol=symbol))
        projected_positions[symbol] = current_pos.quantity + fill_qty

        projected_cash = self.cash
        if fill_qty > 0:
            projected_cash -= fill_qty * fill_price + fees
        elif fill_qty < 0:
            projected_cash += abs(fill_qty) * fill_price - fees

        projected_equity = projected_cash
        for sym, qty in projected_positions.items():
            if qty == 0:
                continue
            price = price_map.get(sym, self.positions.get(sym, Position(symbol=sym)).avg_price)
            projected_equity += qty * price

        projected_max_equity = max(self.max_equity, projected_equity)
        projected_drawdown = (
            (projected_max_equity - projected_equity) / projected_max_equity
            if projected_max_equity > 0
            else 0.0
        )
        projected_daily_pnl = projected_equity - self.daily_start_equity
        return projected_equity, projected_drawdown, projected_daily_pnl

    def validate_order(
        self,
        symbol: str,
        fill_qty: float,
        fill_price: float,
        fees: float = 0.0,
        prices: Optional[Dict[str, float]] = None,
    ) -> Tuple[bool, Optional[str]]:
        if self.kill_switch:
            return False, "kill switch is active"
        if not symbol or not str(symbol).strip():
            return False, "symbol is required"
        if fill_price <= 0:
            return False, "fill price must be positive"
        if fill_qty == 0:
            return False, "trade quantity must be non-zero"
        if not math.isfinite(fill_price) or not math.isfinite(fill_qty):
            return False, "invalid trade values"

        symbol = str(symbol).upper()
        current_pos = self.positions.get(symbol, Position(symbol=symbol))
        if fill_qty < 0 and abs(fill_qty) > current_pos.quantity:
            return False, "sell exceeds current position size"
        if fill_qty > 0 and fill_qty * fill_price + fees > self.cash:
            return False, "insufficient cash for buy order"

        projected_equity, projected_drawdown, projected_daily_pnl = self._projected_risk_state(
            symbol,
            fill_qty,
            fill_price,
            fees=fees,
            prices=prices,
        )

        if projected_drawdown >= self.max_drawdown_limit:
            return False, "max drawdown limit would be breached"

        daily_loss_threshold = self.daily_loss_limit * self.daily_start_equity
        if projected_daily_pnl < -daily_loss_threshold:
            return False, "daily loss limit would be breached"

        projected_positions = {
            sym: pos.quantity for sym, pos in self.positions.items()
        }
        projected_positions[symbol] = current_pos.quantity + fill_qty

        projected_exposure = 0.0
        for sym, qty in projected_positions.items():
            if qty == 0:
                continue
            price = (prices or {}).get(sym, self.positions.get(sym, Position(symbol=sym)).avg_price)
            projected_exposure += abs(qty) * price

        if projected_exposure >= self.max_total_position_value:
            return False, "total position exposure exceeds limit"

        projected_symbol_exposure = abs(projected_positions.get(symbol, 0.0)) * (
            (prices or {}).get(symbol, self.positions.get(symbol, Position(symbol=symbol)).avg_price)
            or fill_price
        )
        if projected_symbol_exposure >= self.max_position_value_per_symbol:
            return False, "per-symbol position limit would be exceeded"

        if projected_equity <= 0:
            return False, "portfolio equity is negative after trade"

        return True, None

    def execute_trade(
        self,
        symbol: str,
        fill_qty: float,
        fill_price: float,
        fees: float = 0.0,
        prices: Optional[Dict[str, float]] = None,
    ) -> bool:
        is_valid, rejection_reason = self.validate_order(
            symbol,
            fill_qty,
            fill_price,
            fees=fees,
            prices=prices,
        )
        if not is_valid:
            self.last_risk_rejection = rejection_reason
            return False

        self.last_risk_rejection = None
        self.update_cash(fill_qty, fill_price, fees)
        self.update_position(symbol, fill_qty, fill_price)
        return True

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
            (self.max_equity - equity) / self.max_equity if self.max_equity > 0 else 0.0
        )

        timestamp = datetime.utcnow().isoformat() + "Z"
        point = {"timestamp": timestamp, "equity": equity}
        self.equity_curve.append(point)

        volatility = self._compute_volatility()
        daily_pnl = equity - self.daily_start_equity

        snapshot = {
            "timestamp": timestamp,
            "cash": self.cash,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "equity": equity,
            "drawdown": drawdown,
            "daily_pnl": daily_pnl,
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
            "kill_switch": self.kill_switch,
            "max_drawdown_limit": self.max_drawdown_limit,
            "daily_loss_limit": self.daily_loss_limit,
            "max_position_value_per_symbol": self.max_position_value_per_symbol,
            "max_total_position_value": self.max_total_position_value,
            "last_risk_rejection": self.last_risk_rejection,
        }
        return snapshot

    def to_json(self) -> str:
        return json.dumps(self.mark_to_market({}), indent=2)
