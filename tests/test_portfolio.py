from simulator.portfolio import Portfolio


def test_portfolio_buy_and_sell_round_trip():
    p = Portfolio(cash=100_000.0)

    p.update_cash(fill_qty=10, fill_price=100.0, fees=1.0)
    p.update_position("AAPL", fill_qty=10, fill_price=100.0)

    metrics_after_buy = p.mark_to_market({"AAPL": 100.0})
    assert metrics_after_buy["cash"] < 100_000.0
    assert any(pos["symbol"] == "AAPL" for pos in metrics_after_buy["positions"])

    p.update_cash(fill_qty=-10, fill_price=101.0, fees=1.0)
    p.update_position("AAPL", fill_qty=-10, fill_price=101.0)

    metrics_after_sell = p.mark_to_market({"AAPL": 101.0})
    assert metrics_after_sell["equity"] <= 100_000.0 + 100


def test_position_limits_reject_excessive_order():
    p = Portfolio(cash=100_000.0, max_position_value_per_symbol=1_000.0)
    assert p.execute_trade("AAPL", 10, 100.0, fees=0.0, prices={"AAPL": 100.0}) is False
    assert p.last_risk_rejection is not None


def test_drawdown_and_daily_loss_rules_reject_order():
    p = Portfolio(cash=100_000.0, max_drawdown_limit=0.10, daily_loss_limit=0.05)
    p.max_equity = 120_000.0
    p.cash = 80_000.0
    p.positions = {"AAPL": type("Pos", (), {"quantity": 200, "avg_price": 100.0})()}

    accepted, reason = p.validate_order("AAPL", 1, 100.0, fees=0.0, prices={"AAPL": 100.0})
    assert accepted is False
    assert "drawdown" in reason.lower()


def test_kill_switch_blocks_new_trades():
    p = Portfolio(cash=100_000.0)
    p.set_kill_switch(True)

    accepted, reason = p.validate_order("AAPL", 1, 100.0, fees=0.0, prices={"AAPL": 100.0})
    assert accepted is False
    assert "kill switch" in reason.lower()
