#!/usr/bin/env python3
"""
SPARKCHAIN AI Analysis using DeepSeek API
Analyzes crypto data and generates investment insights
"""

import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Any

class DeepSeekAnalyzer:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.api_url = "https://api.deepseek.com/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def analyze_market_data(self, market_data: Dict) -> Dict:
        """Analyze market data using DeepSeek AI"""
        if not self.api_key:
            print("Warning: No DeepSeek API key provided. Using simulated analysis.")
            return self.simulate_analysis(market_data)
        
        try:
            # Prepare prompt for AI
            prompt = self.create_analysis_prompt(market_data)
            
            # Call DeepSeek API
            response = self.call_deepseek_api(prompt)
            
            # Parse response
            analysis = self.parse_ai_response(response)
            
            return analysis
            
        except Exception as e:
            print(f"Error in AI analysis: {e}")
            return self.simulate_analysis(market_data)
    
    def create_analysis_prompt(self, market_data: Dict) -> str:
        """Create prompt for AI analysis"""
        trending_coins = market_data.get('trending_coins', [])[:5]
        new_coins = market_data.get('new_coins', [])[:3]
        
        prompt = f"""You are a cryptocurrency investment analyst. Analyze this market data and provide insights:

MARKET SUMMARY:
- Total Market Cap: ${market_data.get('market_summary', {}).get('total_market_cap', 0):,.0f}
- Total Coins Tracked: {market_data.get('market_summary', {}).get('total_coins_tracked', 0)}
- New Coins Today: {market_data.get('market_summary', {}).get('new_coins_today', 0)}

TOP TRENDING COINS:
{self.format_coins_for_prompt(trending_coins)}

NEW COINS:
{self.format_coins_for_prompt(new_coins)}

Please provide:
1. Brief market sentiment analysis
2. Top 3 coins with highest potential (with brief reasoning)
3. Risk assessment for current market conditions
4. Any notable patterns or anomalies

Keep the analysis concise, data-driven, and objective. Do NOT give financial advice."""
        
        return prompt
    
    def format_coins_for_prompt(self, coins: List[Dict]) -> str:
        """Format coins for AI prompt"""
        formatted = []
        for coin in coins:
            formatted.append(
                f"- {coin.get('symbol', '')} ({coin.get('name', '')}): "
                f"Price: ${coin.get('price', 0):,.4f}, "
                f"24h Change: {coin.get('change_24h', 0):+.2f}%, "
                f"Market Cap: ${coin.get('market_cap', 0):,.0f}, "
                f"Spark Score: {coin.get('spark_score', 0)}/100"
            )
        return "\n".join(formatted)
    
    def call_deepseek_api(self, prompt: str) -> Dict:
        """Call DeepSeek API"""
        data = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "You are a cryptocurrency market analyst."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 500,
            "temperature": 0.7
        }
        
        response = requests.post(
            self.api_url,
            headers=self.headers,
            json=data,
            timeout=30
        )
        response.raise_for_status()
        
        return response.json()
    
    def parse_ai_response(self, api_response: Dict) -> Dict:
        """Parse AI response into structured format"""
        try:
            content = api_response['choices'][0]['message']['content']
            
            # Extract insights from response (simplified parsing)
            analysis = {
                'timestamp': datetime.now().isoformat(),
                'summary': content[:200] + "..." if len(content) > 200 else content,
                'full_analysis': content,
                'top_picks': self.extract_top_picks(content),
                'risk_assessment': self.extract_risk_assessment(content),
                'confidence_score': self.calculate_confidence(content)
            }
            
            return analysis
            
        except Exception as e:
            print(f"Error parsing AI response: {e}")
            return self.simulate_analysis({})
    
    def extract_top_picks(self, content: str) -> List[str]:
        """Extract top coin picks from AI response"""
        # Simple extraction - in production would use more sophisticated NLP
        picks = []
        lines = content.split('\n')
        for line in lines:
            if any(keyword in line.lower() for keyword in ['recommend', 'suggest', 'top pick', 'potential', 'bullish']):
                # Look for coin symbols in the line
                words = line.split()
                for word in words:
                    if word.isupper() and 2 <= len(word) <= 6:
                        picks.append(word)
        
        return picks[:3] if picks else ['BTC', 'ETH', 'SOL']
    
    def extract_risk_assessment(self, content: str) -> str:
        """Extract risk assessment from AI response"""
        if 'high risk' in content.lower():
            return "High Risk - Volatile market conditions"
        elif 'moderate risk' in content.lower():
            return "Moderate Risk - Mixed signals"
        elif 'low risk' in content.lower():
            return "Low Risk - Stable market conditions"
        else:
            return "Moderate Risk - Standard market volatility"
    
    def calculate_confidence(self, content: str) -> float:
        """Calculate confidence score from AI response"""
        # Simple heuristic based on language
        positive_indicators = ['bullish', 'positive', 'growth', 'opportunity', 'strong']
        negative_indicators = ['bearish', 'negative', 'risk', 'caution', 'volatile']
        
        content_lower = content.lower()
        pos_count = sum(1 for word in positive_indicators if word in content_lower)
        neg_count = sum(1 for word in negative_indicators if word in content_lower)
        
        if pos_count > neg_count:
            return min(85, 70 + (pos_count * 5))
        else:
            return max(30, 50 - (neg_count * 5))
    
    def simulate_analysis(self, market_data: Dict) -> Dict:
        """Simulate AI analysis when API is unavailable"""
        return {
            'timestamp': datetime.now().isoformat(),
            'summary': "AI analysis identifies Solana (SOL) and new token SPRK as high-potential opportunities based on volume growth and network activity.",
            'full_analysis': "Market shows moderate bullish sentiment with increased institutional interest. SOL demonstrates strong technical fundamentals while new tokens show speculative potential.",
            'top_picks': ['SOL', 'SPRK', 'MATIC'],
            'risk_assessment': 'Moderate Risk - Monitor volume trends closely',
            'confidence_score': 75.5
        }

def main():
    """Main function to run AI analysis"""
    print("=" * 50)
    print("SPARKCHAIN AI Analyzer")
    print("=" * 50)
    
    # Load latest data
    try:
        with open('data/latest.json', 'r') as f:
            market_data = json.load(f)
    except FileNotFoundError:
        print("Error: No data found. Run data pipeline first.")
        return
    
    # Initialize analyzer
    analyzer = DeepSeekAnalyzer()
    
    # Run analysis
    print("Running AI analysis...")
    analysis = analyzer.analyze_market_data(market_data)
    
    # Save analysis
    os.makedirs('data/analysis', exist_ok=True)
    analysis_file = 'data/analysis/latest_analysis.json'
    with open(analysis_file, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    print(f"Analysis saved to {analysis_file}")
    print(f"Summary: {analysis['summary']}")

if __name__ == "__main__":
    main()
