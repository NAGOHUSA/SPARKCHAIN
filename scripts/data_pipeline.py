#!/usr/bin/env python3
"""
SPARKCHAIN Data Pipeline with Predictive Analysis
Fetches crypto data from CoinGecko and generates predictive insights
"""

import requests
import json
import os
import sys
from datetime import datetime, timedelta
import numpy as np

# Add parent directory to path to import from root if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration - FIXED PATHS for GitHub Actions
CONFIG = {
    "data_dir": os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"),
    "coingecko_api": "https://api.coingecko.com/api/v3",
    "coingecko_params": {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d,14d,30d"
    },
    "min_market_cap": 1000000,
    "min_volume": 100000
}

def fetch_coingecko_data():
    """Fetch data from CoinGecko API"""
    try:
        url = f"{CONFIG['coingecko_api']}/coins/markets"
        response = requests.get(url, params=CONFIG['coingecko_params'], timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching CoinGecko data: {e}")
        return []

def calculate_spark_score(coin):
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
    if market_cap > 1000000000:
        cap_score = 20
    elif market_cap > 100000000:
        cap_score = 15
    elif market_cap > 10000000:
        cap_score = 10
    else:
        cap_score = 5
    scores.append(cap_score)
    
    # Liquidity (max 15 points)
    if volume > 10000000:
        liquidity_score = 15
    elif volume > 1000000:
        liquidity_score = 10
    elif volume > 100000:
        liquidity_score = 5
    else:
        liquidity_score = 0
    scores.append(liquidity_score)
    
    # Stability (max 10 points)
    stability_score = 7
    scores.append(stability_score)
    
    return min(100, sum(scores))

def calculate_prediction_score(coin_data):
    """Calculate predictive score for future trends (0-100)"""
    score = 50  # Base score
    
    # Factor 1: Current momentum
    change_24h = coin_data.get('change24h', 0)
    if change_24h > 0:
        score += min(20, change_24h * 0.5)
    
    # Factor 2: Volume momentum
    volume_ratio = coin_data.get('volume24h', 0) / max(1, coin_data.get('marketCap', 1))
    score += min(15, volume_ratio * 1000)
    
    # Factor 3: Market position
    market_cap = coin_data.get('marketCap', 0)
    if 10000000 < market_cap < 1000000000:  # Sweet spot for growth
        score += 10
    elif market_cap < 10000000:  # Micro-cap potential
        score += 15
    
    # Factor 4: Price stability (higher price = more stable)
    price = coin_data.get('price', 0)
    if price > 10:
        score += 5
    elif price > 1:
        score += 3
    
    # Factor 5: Historical consistency (simulated)
    score += np.random.uniform(0, 10)
    
    return min(100, max(0, score))

def predict_future_change(coin_data):
    """Predict future price changes based on current data"""
    volatility_factor = 0.15 if coin_data.get('marketCap', 0) > 100000000 else 0.25
    
    # Base prediction on current momentum
    current_change = coin_data.get('change24h', 0)
    base_short_term = current_change * 0.7 if current_change > 0 else 2
    base_mid_term = current_change * 1.5 if current_change > 0 else 5
    
    # Add random variation based on volatility
    random_short = (np.random.random() - 0.5) * volatility_factor * 100
    random_mid = (np.random.random() - 0.5) * volatility_factor * 150
    
    short_term = base_short_term + random_short
    mid_term = base_mid_term + random_mid
    
    # Cap predictions to reasonable ranges
    return {
        'short_term': max(-20, min(50, short_term)),
        'mid_term': max(-30, min(100, mid_term))
    }

def get_prediction_factors(coin_data):
    """Generate key factors for predictions"""
    factors = []
    
    # Based on data characteristics
    if coin_data.get('change24h', 0) > 10:
        factors.append('Strong momentum')
    
    volume_ratio = coin_data.get('volume24h', 0) / max(1, coin_data.get('marketCap', 1))
    if volume_ratio > 0.05:
        factors.append('High volume ratio')
    
    if coin_data.get('marketCap', 0) < 500000000:
        factors.append('Growth potential')
    
    if coin_data.get('price', 0) > 1:
        factors.append('Price stability')
    
    # Additional random factors for demo
    demo_factors = [
        'Technical breakout pattern',
        'Increasing adoption rate',
        'Positive network growth',
        'Strong community sentiment',
        'Development milestones',
        'Exchange listings upcoming'
    ]
    
    while len(factors) < 3:
        new_factor = np.random.choice(demo_factors)
        if new_factor not in factors:
            factors.append(new_factor)
    
    return factors[:3]

def identify_new_coins(existing_data, new_data):
    """Identify newly listed coins"""
    if not existing_data:
        return []
    
    existing_symbols = {coin['symbol'].upper() for coin in existing_data}
    new_coins = []
    
    for coin in new_data:
        if coin['symbol'].upper() not in existing_symbols:
            if (coin.get('marketCap', 0) >= CONFIG['min_market_cap'] and 
                coin.get('volume24h', 0) >= CONFIG['min_volume']):
                new_coins.append(coin)
    
    return new_coins[:5]

def process_data():
    """Main data processing function"""
    print(f"[{datetime.now()}] Starting data collection...")
    
    # Fetch data from CoinGecko
    coingecko_data = fetch_coingecko_data()
    
    if not coingecko_data:
        print("No data fetched from CoinGecko")
        return {}, {}
    
    # Process CoinGecko data with CORRECT camelCase keys
    processed_coins = []
    for coin in coingecko_data:
        processed_coin = {
            'symbol': coin.get('symbol', '').upper(),
            'name': coin.get('name', ''),
            'price': coin.get('current_price', 0),
            'change24h': coin.get('price_change_percentage_24h', 0),
            'marketCap': coin.get('market_cap', 0),
            'volume24h': coin.get('total_volume', 0),
            'sparkScore': calculate_spark_score(coin)
        }
        
        # Handle None values
        for key in ['price', 'change24h', 'marketCap', 'volume24h', 'sparkScore']:
            if processed_coin[key] is None:
                processed_coin[key] = 0
        
        processed_coins.append(processed_coin)
    
    # Sort by Spark Score
    processed_coins.sort(key=lambda x: x['sparkScore'], reverse=True)
    
    # Identify trending coins (top 20 by score)
    trending_coins = processed_coins[:20]
    
    # Generate predictive analysis
    predictions = []
    for coin in processed_coins[:50]:  # Use top 50 for predictions
        prediction_score = calculate_prediction_score(coin)
        future_change = predict_future_change(coin)
        
        prediction = {
            'symbol': coin['symbol'],
            'name': coin['name'],
            'price': coin['price'],
            'change24h': coin['change24h'],
            'marketCap': coin['marketCap'],
            'predictionScore': round(prediction_score, 1),
            'predictedChange7d': round(future_change['short_term'], 1),
            'predictedChange30d': round(future_change['mid_term'], 1),
            'confidence': 'high' if prediction_score > 80 else 'medium' if prediction_score > 60 else 'low',
            'factors': get_prediction_factors(coin),
            'timestamp': datetime.now().isoformat()
        }
        predictions.append(prediction)
    
    # Sort predictions by score
    predictions.sort(key=lambda x: x['predictionScore'], reverse=True)
    
    # Calculate market summary
    total_market_cap = sum(coin['marketCap'] for coin in processed_coins)
    total_volume = sum(coin['volume24h'] for coin in processed_coins)
    
    # Load existing data to identify new coins
    existing_data = []
    try:
        latest_file = os.path.join(CONFIG['data_dir'], 'latest.json')
        if os.path.exists(latest_file):
            with open(latest_file, 'r') as f:
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
            'market_cap_change_24h': 0,
            'total_volume_24h': total_volume,
            'volume_change_24h': 0,
            'total_coins_tracked': len(processed_coins),
            'new_coins_today': len(new_coins),
            'trending_coins_count': len(trending_coins),
            'trending_growth': 0
        },
        'trending_coins': trending_coins,
        'new_coins': new_coins,
        'all_coins': processed_coins[:50],
        'predictions_available': len(predictions) > 0
    }
    
    # Try to calculate 24h changes from historical data
    try:
        historical_dir = os.path.join(CONFIG['data_dir'], 'historical')
        if os.path.exists(historical_dir):
            historical_files = sorted(os.listdir(historical_dir))
            if len(historical_files) > 0:
                latest_file = os.path.join(historical_dir, historical_files[-1])
                with open(latest_file, 'r') as f:
                    historical_data = json.load(f)
                    hist_total_market_cap = historical_data.get('market_summary', {}).get('total_market_cap', total_market_cap)
                    hist_total_volume = historical_data.get('market_summary', {}).get('total_volume_24h', total_volume)
                    
                    if hist_total_market_cap > 0:
                        result['market_summary']['market_cap_change_24h'] = round(
                            (total_market_cap - hist_total_market_cap) / hist_total_market_cap * 100, 2
                        )
                    
                    if hist_total_volume > 0:
                        result['market_summary']['volume_change_24h'] = round(
                            (total_volume - hist_total_volume) / hist_total_volume * 100, 2
                        )
    except:
        pass
    
    # Save predictions separately
    predictions_data = {
        'timestamp': datetime.now().isoformat(),
        'top_predictions': predictions[:6],  # Top 6 for display
        'all_predictions': predictions[:20],  # Top 20 for reference
        'prediction_metrics': {
            'average_7d_prediction': round(np.mean([p['predictedChange7d'] for p in predictions[:10]]), 1),
            'average_30d_prediction': round(np.mean([p['predictedChange30d'] for p in predictions[:10]]), 1),
            'high_confidence_count': sum(1 for p in predictions if p['confidence'] == 'high'),
            'total_predicted': len(predictions)
        }
    }
    
    print(f"[{datetime.now()}] Data collection complete. Found {len(trending_coins)} trending coins.")
    print(f"[{datetime.now()}] Generated {len(predictions)} predictive analyses.")
    
    return result, predictions_data

def save_data(market_data, predictions_data):
    """Save data to files"""
    os.makedirs(CONFIG['data_dir'], exist_ok=True)
    os.makedirs(os.path.join(CONFIG['data_dir'], 'historical'), exist_ok=True)
    os.makedirs(os.path.join(CONFIG['data_dir'], 'analysis'), exist_ok=True)
    
    # Save market data
    latest_file = os.path.join(CONFIG['data_dir'], 'latest.json')
    with open(latest_file, 'w') as f:
        json.dump(market_data, f, indent=2)
    
    # Save predictions
    predictions_file = os.path.join(CONFIG['data_dir'], 'predictions.json')
    with open(predictions_file, 'w') as f:
        json.dump(predictions_data, f, indent=2)
    
    # Save historical snapshot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    historical_file = os.path.join(CONFIG['data_dir'], 'historical', f'data_{timestamp}.json')
    with open(historical_file, 'w') as f:
        json.dump(market_data, f, indent=2)
    
    # Clean up old data
    cleanup_old_data()

def cleanup_old_data():
    """Remove historical data older than 24 hours"""
    historical_dir = os.path.join(CONFIG['data_dir'], 'historical')
    if os.path.exists(historical_dir):
        now = datetime.now()
        for filename in os.listdir(historical_dir):
            if filename.startswith('data_') and filename.endswith('.json'):
                try:
                    timestamp_str = filename[5:-5]
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    if (now - file_time) > timedelta(hours=24):
                        os.remove(os.path.join(historical_dir, filename))
                except:
                    pass

def main():
    """Main function"""
    print("=" * 50)
    print("SPARKCHAIN Data Pipeline with Predictive Analysis")
    print("=" * 50)
    
    market_data, predictions_data = process_data()
    if market_data:
        save_data(market_data, predictions_data)
        print(f"[{datetime.now()}] Data saved successfully")
        print(f"[{datetime.now()}] Predictions saved successfully")
    else:
        print(f"[{datetime.now()}] No data to save")

if __name__ == "__main__":
    main()
