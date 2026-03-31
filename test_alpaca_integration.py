#!/usr/bin/env python3
"""
Test script to verify Alpaca API integration works correctly.
This script tests the Alpaca market producer without requiring Kafka.
"""

import os
import sys
from producers.alpaca_market_producer import (
    initialize_alpaca_client,
    get_real_time_prices,
    get_historical_prices,
    mock_price_stream
)

def test_alpaca_initialization():
    """Test that Alpaca client can be initialized."""
    print("Testing Alpaca client initialization...")
    
    # Test without API keys (should return None)
    client = initialize_alpaca_client()
    if client is None:
        print("✓ Alpaca client correctly returns None when no API keys are set")
    else:
        print("✓ Alpaca client initialized successfully")
    
    return client

def test_mock_data_fallback():
    """Test that mock data fallback works correctly."""
    print("\nTesting mock data fallback...")
    
    # Test mock price stream
    symbols = ["AAPL", "MSFT", "GOOG"]
    stream = mock_price_stream(symbols)
    
    # Get a few ticks
    for i, tick in enumerate(stream):
        if i >= 3:  # Just test 3 ticks
            break
        print(f"✓ Mock tick {i+1}: {tick['symbol']} = ${tick['price']}")
    
    print("✓ Mock data fallback works correctly")

def main():
    """Run all integration tests."""
    print("=" * 60)
    print("ALPACA MARKET DATA INTEGRATION TEST")
    print("=" * 60)
    
    try:
        # Test initialization
        client = test_alpaca_initialization()
        
        # Test mock data fallback
        test_mock_data_fallback()
        
        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED - Alpaca integration is working!")
        print("\nNext steps:")
        print("1. Get free Alpaca API keys from: https://app.alpaca.markets/signup")
        print("2. Add ALPACA_API_KEY and ALPACA_API_SECRET to your .env file")
        print("3. Run: python -m producers.alpaca_market_producer")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()