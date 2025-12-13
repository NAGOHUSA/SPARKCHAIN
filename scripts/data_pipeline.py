def process_data() -> Dict:
    """Main data processing function"""
    print(f"[{datetime.now()}] Starting data collection...")
    
    # Fetch data from APIs
    coingecko_data = fetch_coingecko_data()
    
    if not coingecko_data:
        print("No data fetched from APIs")
        return {}
    
    # Process CoinGecko data - MATCHING JAVASCRIPT FORMAT
    processed_coins = []
    for coin in coingecko_data:
        processed_coin = {
            'id': coin.get('id', ''),
            'symbol': coin.get('symbol', '').upper(),
            'name': coin.get('name', ''),
            'price': coin.get('current_price', 0),
            'change24h': coin.get('price_change_percentage_24h', 0),  # Changed from change_24h
            'marketCap': coin.get('market_cap', 0),  # Changed from market_cap
            'volume24h': coin.get('total_volume', 0),  # Changed from volume_24h
            'circulating_supply': coin.get('circulating_supply', 0),
            'ath': coin.get('ath', 0),
            'ath_change_percentage': coin.get('ath_change_percentage', 0),
            'last_updated': coin.get('last_updated', '')
        }
        
        # Calculate Spark Score - UPDATED KEY NAME
        processed_coin['sparkScore'] = calculate_spark_score(coin)  # Changed from spark_score
        processed_coins.append(processed_coin)
    
    # Sort by Spark Score (descending)
    processed_coins.sort(key=lambda x: x['sparkScore'], reverse=True)
    
    # Identify trending coins (top 20 by score)
    trending_coins = processed_coins[:20]
    
    # Calculate market summary
    total_market_cap = sum(coin['marketCap'] for coin in processed_coins)
    total_volume = sum(coin['volume24h'] for coin in processed_coins)
    
    # Try to load existing data to identify new coins
    existing_data = []
    try:
        with open(os.path.join(CONFIG['data_dir'], 'latest.json'), 'r') as f:
            existing = json.load(f)
            existing_data = existing.get('trending_coins', [])
    except:
        pass
    
    # Update identify_new_coins to use new key names
    existing_symbols = {coin['symbol'].upper() for coin in existing_data}
    new_coins = []
    for coin in processed_coins:
        if coin['symbol'].upper() not in existing_symbols:
            if (coin.get('marketCap', 0) >= CONFIG['min_market_cap'] and 
                coin.get('volume24h', 0) >= CONFIG['min_volume']):
                new_coins.append(coin)
    new_coins = new_coins[:10]
    
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
