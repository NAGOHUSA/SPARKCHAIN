#!/usr/bin/env python3
"""
SPARKCHAIN Data Pipeline
Fetches crypto data from multiple sources and processes it
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, List, Any

# Configuration
CONFIG = {
    "data_dir": "data",
    "coingecko_api": "https://api.coingecko.com/api/v3",
    "coingecko_params": {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d"
    },
    "cryptocompare_api": "https://min-api.cryptocompare.com/data",
    "cryptocompare_key": os.getenv("CRYPTOCOMPARE_KEY", ""),
    "update_interval": 3600,  # 1 hour
    "min_market_cap": 1000000,  # $1M minimum
    "min_volume": 100000  # $100K minimum
}

def fetch_coingecko_data() -> List[Dict]:
    """Fetch data from CoinGecko API"""
    try:
        url = f"{CONFIG['coingecko_api']}/coins/markets"
        response = requests.get(url, params=CONFIG['coingecko_params'], timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching CoinGecko data: {e}")
        return []

def fetch_cryptocompare_data() -> Dict:
    """Fetch additional data from CryptoCompare"""
    try:
        url = f"{CONFIG['cryptocompare_api']}/top/totalvolfull"
        params = {
            "limit": 50,
            "tsym": "USD",
            "api_key": CONFIG['cryptocompare_key']
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching CryptoCompare data: {e}")
        return {}

def calculate_spark_score(coin: Dict) -> float:
    """Calculate Spark Score for a coin (0-100)"""
    scores = []
    
    # Price momentum (max 30 points)
    if 'price_change_percentage_24h' in coin:
        change_24h = abs(coin.get('price_change_percentage_24h', 0))
        momentum_score = min(30, change_24h * 0.3)
        scores.append(momentum_score)
    
    # Volume/Market Cap ratio (max 25 points)
    market_cap = coin.get('market_cap', 0)
    volume = coin.get('total_volume', 0)
    if market_cap > 0 and volume > 0:
        volume_ratio = (volume / market_cap) * 100
        volume_score = min(25, volume_ratio * 5)
        scores.append(volume_score)
    
    # Market cap tier (max 20 points)
    if market_cap > 1000000000:  # > $1B
        cap_score = 20
    elif market_cap > 100000000:  # > $100M
        cap_score = 15
    elif market_cap > 10000000:   # > $10M
        cap_score = 10
    else:
        cap_score = 5
    scores.append(cap_score)
    
    # Liquidity (max 15 points)
    if volume > 10000000:  # > $10M volume
        liquidity_score = 15
    elif volume > 1000000:  # > $1M volume
        liquidity_score = 10
    elif volume > 100000:   # > $100K volume
        liquidity_score = 5
    else:
        liquidity_score = 0
    scores.append(liquidity_score)
    
    # Age/Stability (max 10 points)
    # Simulated - in production would check coin age
    stability_score = 7
    scores.append(stability_score)
    
    return min(100, sum(scores))

def identify_new_coins(existing_data: List[Dict], new_data: List[Dict]) -> List[Dict]:
    """Identify newly listed coins"""
    existing_symbols = {coin['symbol'].upper() for coin in existing_data}
    new_coins = []
    
    for coin in new_data:
        if coin['symbol'].upper() not in existing_symbols:
            # Check if meets minimum criteria
            if (coin.get('market_cap', 0) >= CONFIG['min_market_cap'] and 
                coin.get('total_volume', 0) >= CONFIG['min_volume']):
                new_coins.append(coin)
    
    return new_coins[:10]  # Return top 10 new coins

def process_data() -> Dict:
    """Main data processing function"""
    print(f"[{datetime.now()}] Starting data collection...")
    
    # Fetch data from APIs
    coingecko_data = fetch_coingecko_data()
    cryptocompare_data = fetch_cryptocompare_data()
    
    if not coingecko_data:
        print("No data fetched from APIs")
        return {}
    
    # Process CoinGecko data
    processed_coins = []
    for coin in coingecko_data:
        processed_coin = {
            'id': coin.get('id', ''),
            'symbol': coin.get('symbol', '').upper(),
            'name': coin.get('name', ''),
            'price': coin.get('current_price', 0),
            'change_24h': coin.get('price_change_percentage_24h', 0),
            'market_cap': coin.get('market_cap', 0),
            'volume_24h': coin.get('total_volume', 0),
            'circulating_supply': coin.get('circulating_supply', 0),
            'ath': coin.get('ath', 0),
            'ath_change_percentage': coin.get('ath_change_percentage', 0),
            'last_updated': coin.get('last_updated', '')
        }
        
        # Calculate Spark Score
        processed_coin['spark_score'] = calculate_spark_score(coin)
        processed_coins.append(processed_coin)
    
    # Sort by Spark Score (descending)
    processed_coins.sort(key=lambda x: x['spark_score'], reverse=True)
    
    # Identify trending coins (top 20 by score)
    trending_coins = processed_coins[:20]
    
    # Calculate market summary
    total_market_cap = sum(coin['market_cap'] for coin in processed_coins)
    total_volume = sum(coin['volume_24h'] for coin in processed_coins)
    
    # Try to load existing data to identify new coins
    existing_data = []
    try:
        with open(os.path.join(CONFIG['data_dir'], 'latest.json'), 'r') as f:
            existing = json.load(f)
            existing_data = existing.get('trending_coins', [])
    except:
        pass
    
    new_coins = identify_new_coins(existing_data, processed_coins)
    
    # Prepare final data structure
    result = {
        'timestamp': datetime.now().isoformat(),
        'market_summary': {
            'total_market_cap': total_market_cap,
            'market_cap_change_24h': 2.8,  # Would calculate from historical
            'total_volume_24h': total_volume,
            'volume_change_24h': 12.5,  # Would calculate from historical
            'total_coins_tracked': len(processed_coins),
            'new_coins_today': len(new_coins),
            'trending_coins_count': len(trending_coins),
            'trending_growth': 8.7  # Would calculate
        },
        'trending_coins': trending_coins,
        'new_coins': new_coins,
        'all_coins': processed_coins[:100]  # Top 100 only
    }
    
    print(f"[{datetime.now()}] Data collection complete. Found {len(trending_coins)} trending coins.")
    return result

def save_data(data: Dict):
    """Save data to files"""
    os.makedirs(CONFIG['data_dir'], exist_ok=True)
    os.makedirs(os.path.join(CONFIG['data_dir'], 'historical'), exist_ok=True)
    os.makedirs(os.path.join(CONFIG['data_dir'], 'analysis'), exist_ok=True)
    
    # Save latest data
    latest_file = os.path.join(CONFIG['data_dir'], 'latest.json')
    with open(latest_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Save historical snapshot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    historical_file = os.path.join(CONFIG['data_dir'], 'historical', f'data_{timestamp}.json')
    with open(historical_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Keep only last 24 hours of historical data
    cleanup_old_data()

def cleanup_old_data():
    """Remove historical data older than 24 hours"""
    historical_dir = os.path.join(CONFIG['data_dir'], 'historical')
    if os.path.exists(historical_dir):
        now = datetime.now()
        for filename in os.listdir(historical_dir):
            if filename.startswith('data_') and filename.endswith('.json'):
                try:
                    timestamp_str = filename[5:-5]  # Remove 'data_' and '.json'
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    if (now - file_time) > timedelta(hours=24):
                        os.remove(os.path.join(historical_dir, filename))
                except:
                    pass

def main():
    """Main function"""
    print("=" * 50)
    print("SPARKCHAIN Data Pipeline")
    print("=" * 50)
    
    data = process_data()
    if data:
        save_data(data)
        print(f"[{datetime.now()}] Data saved successfully")
    else:
        print(f"[{datetime.now()}] No data to save")

if __name__ == "__main__":
    main()
