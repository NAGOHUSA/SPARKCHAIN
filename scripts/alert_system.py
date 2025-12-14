#!/usr/bin/env python3
"""
SPARKCHAIN Alert System
Monitors market conditions and triggers alerts
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Any
import requests

class AlertSystem:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.alerts_file = os.path.join(data_dir, "alerts.json")
        self.alerts = self.load_alerts()
        
    def load_alerts(self) -> List[Dict]:
        """Load saved alerts from file"""
        try:
            if os.path.exists(self.alerts_file):
                with open(self.alerts_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return []
    
    def save_alerts(self):
        """Save alerts to file"""
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.alerts_file, 'w') as f:
            json.dump(self.alerts, f, indent=2)
    
    def create_alert(self, alert_data: Dict) -> str:
        """Create a new alert"""
        alert_id = f"alert_{int(time.time())}_{len(self.alerts)}"
        alert = {
            "id": alert_id,
            "created": datetime.now().isoformat(),
            "active": True,
            "triggered": False,
            "triggered_at": None,
            **alert_data
        }
        self.alerts.append(alert)
        self.save_alerts()
        return alert_id
    
    def check_alerts(self, market_data: Dict) -> List[Dict]:
        """Check all alerts against current market data"""
        triggered_alerts = []
        
        for alert in self.alerts:
            if not alert.get("active") or alert.get("triggered"):
                continue
            
            if self.evaluate_alert(alert, market_data):
                alert["triggered"] = True
                alert["triggered_at"] = datetime.now().isoformat()
                triggered_alerts.append(alert)
        
        if triggered_alerts:
            self.save_alerts()
            self.log_triggered_alerts(triggered_alerts)
        
        return triggered_alerts
    
    def evaluate_alert(self, alert: Dict, market_data: Dict) -> bool:
        """Evaluate if an alert condition is met"""
        alert_type = alert.get("type", "price")
        
        if alert_type == "price":
            return self.evaluate_price_alert(alert, market_data)
        elif alert_type == "volume":
            return self.evaluate_volume_alert(alert, market_data)
        elif alert_type == "change":
            return self.evaluate_change_alert(alert, market_data)
        elif alert_type == "sentiment":
            return self.evaluate_sentiment_alert(alert, market_data)
        
        return False
    
    def evaluate_price_alert(self, alert: Dict, market_data: Dict) -> bool:
        """Evaluate price-based alert"""
        symbol = alert.get("symbol", "").upper()
        condition = alert.get("condition", "above")
        threshold = float(alert.get("value", 0))
        
        # Find coin in market data
        coins = market_data.get("trending_coins", [])
        coin = next((c for c in coins if c["symbol"] == symbol), None)
        
        if not coin:
            return False
        
        current_price = coin.get("price", 0)
        
        if condition == "above":
            return current_price > threshold
        elif condition == "below":
            return current_price < threshold
        elif condition == "crosses_above":
            previous_price = alert.get("last_price", current_price)
            alert["last_price"] = current_price
            return previous_price <= threshold < current_price
        elif condition == "crosses_below":
            previous_price = alert.get("last_price", current_price)
            alert["last_price"] = current_price
            return previous_price >= threshold > current_price
        
        return False
    
    def evaluate_volume_alert(self, alert: Dict, market_data: Dict) -> bool:
        """Evaluate volume-based alert"""
        symbol = alert.get("symbol", "").upper()
        threshold = float(alert.get("value", 0))
        
        coins = market_data.get("trending_coins", [])
        coin = next((c for c in coins if c["symbol"] == symbol), None)
        
        if not coin:
            return False
        
        current_volume = coin.get("volume24h", 0)
        avg_volume = alert.get("avg_volume", current_volume)
        
        # Check for volume spike (e.g., 200% of average)
        if current_volume > avg_volume * (threshold / 100):
            # Update average volume (simple moving average)
            alert["avg_volume"] = (avg_volume * 0.7) + (current_volume * 0.3)
            return True
        
        return False
    
    def evaluate_change_alert(self, alert: Dict, market_data: Dict) -> bool:
        """Evaluate 24h change alert"""
        symbol = alert.get("symbol", "").upper()
        condition = alert.get("condition", "increase")
        threshold = float(alert.get("value", 0))
        
        coins = market_data.get("trending_coins", [])
        coin = next((c for c in coins if c["symbol"] == symbol), None)
        
        if not coin:
            return False
        
        change = coin.get("change24h", 0)
        
        if condition == "increase":
            return change > threshold
        elif condition == "decrease":
            return change < -threshold
        elif condition == "volatility":
            return abs(change) > threshold
        
        return False
    
    def evaluate_sentiment_alert(self, alert: Dict, market_data: Dict) -> bool:
        """Evaluate sentiment-based alert"""
        try:
            sentiment_file = os.path.join(self.data_dir, "sentiment.json")
            if os.path.exists(sentiment_file):
                with open(sentiment_file, 'r') as f:
                    sentiment_data = json.load(f)
                
                threshold = float(alert.get("value", 50))
                current_sentiment = sentiment_data.get("overall_sentiment", 50)
                
                if alert.get("condition") == "above":
                    return current_sentiment > threshold
                elif alert.get("condition") == "below":
                    return current_sentiment < threshold
        except:
            pass
        
        return False
    
    def log_triggered_alerts(self, alerts: List[Dict]):
        """Log triggered alerts to file"""
        log_file = os.path.join(self.data_dir, "alert_log.json")
        log_data = []
        
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    log_data = json.load(f)
        except:
            pass
        
        for alert in alerts:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "alert_id": alert.get("id"),
                "symbol": alert.get("symbol"),
                "type": alert.get("type"),
                "condition": alert.get("condition"),
                "value": alert.get("value"),
                "triggered_value": self.get_triggered_value(alert)
            }
            log_data.append(log_entry)
        
        # Keep only last 1000 entries
        log_data = log_data[-1000:]
        
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2)
    
    def get_triggered_value(self, alert: Dict) -> Any:
        """Get the value that triggered the alert"""
        alert_type = alert.get("type")
        
        if alert_type == "price":
            return alert.get("last_price", 0)
        elif alert_type == "volume":
            return alert.get("avg_volume", 0)
        elif alert_type == "change":
            return alert.get("last_change", 0)
        elif alert_type == "sentiment":
            return alert.get("last_sentiment", 50)
        
        return None
    
    def get_active_alerts(self) -> List[Dict]:
        """Get all active alerts"""
        return [a for a in self.alerts if a.get("active") and not a.get("triggered")]
    
    def get_alert_history(self, limit: int = 50) -> List[Dict]:
        """Get alert history"""
        try:
            log_file = os.path.join(self.data_dir, "alert_log.json")
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    log_data = json.load(f)
                return log_data[-limit:]
        except:
            pass
        return []
    
    def delete_alert(self, alert_id: str) -> bool:
        """Delete an alert"""
        original_count = len(self.alerts)
        self.alerts = [a for a in self.alerts if a.get("id") != alert_id]
        
        if len(self.alerts) < original_count:
            self.save_alerts()
            return True
        return False
    
    def clear_all_alerts(self):
        """Clear all alerts"""
        self.alerts = []
        self.save_alerts()

# Webhook notification function
def send_webhook_notification(webhook_url: str, alert: Dict):
    """Send alert notification to webhook"""
    try:
        payload = {
            "content": f"🚨 **Alert Triggered**: {alert.get('symbol')}",
            "embeds": [{
                "title": f"{alert.get('symbol')} {alert.get('condition')} {alert.get('value')}",
                "color": 16711680,  # Red color
                "fields": [
                    {"name": "Alert Type", "value": alert.get('type', 'unknown'), "inline": True},
                    {"name": "Triggered At", "value": alert.get('triggered_at', 'unknown'), "inline": True}
                ],
                "timestamp": datetime.now().isoformat()
            }]
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        return response.status_code == 200
    except:
        return False
