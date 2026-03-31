from simulator.portfolio import Portfolio


def test_portfolio_buy_and_sell_round_trip():
    p = Portfolio(cash=100_000.0)

    # Buy 10 shares at 100
    p.update_cash(fill_qty=10, fill_price=100.0, fees=1.0)
    p.update_position("AAPL", fill_qty=10, fill_price=100.0)

    metrics_after_buy = p.mark_to_market({"AAPL": 100.0})
    assert metrics_after_buy["cash"] < 100_000.0
    assert any(pos["symbol"] == "AAPL" for pos in metrics_after_buy["positions"])

    # Sell 10 shares at 101 -> small profit
    p.update_cash(fill_qty=-10, fill_price=101.0, fees=1.0)
    p.update_position("AAPL", fill_qty=-10, fill_price=101.0)

    metrics_after_sell = p.mark_to_market({"AAPL": 101.0})
    assert metrics_after_sell["equity"] <= 100_000.0 + 100  # very loose upper bound
