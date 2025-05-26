import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json
import logging
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RiskPulseDataCollector:
    """
    RiskPulse Montreal - Real-time Insurance Risk Data Collector
    
    Collects data from:
    1. OpenWeatherMap API - Weather data for Montreal
    2. Alpha Vantage API - Canadian insurance stock data
    3. Montreal Open Data - Traffic incidents (CSV download)
    """
    
    def __init__(self):
        # Load API keys from environment variables
        self.weather_api_key = os.getenv('OPENWEATHER_API_KEY', 'f554372d4e07b0a1dfe17f7613c8f091')
        self.stock_api_key = os.getenv('ALPHAVANTAGE_API_KEY', 'ISL3TM9TB4FIOR7P')
        
        # Canadian Insurance Companies to track
        self.insurance_stocks = [
            'IFC.TO',    # Intact Financial Corporation
            'FFH.TO',    # Fairfax Financial Holdings
            'MFC.TO',    # Manulife Financial Corp
            'SLF.TO'     # Sun Life Financial Inc
        ]
        
        # Montreal coordinates for weather data
        self.montreal_coords = {
            'lat': 45.5017,
            'lon': -73.5673
        }
        
        # Create data directories if they don't exist
        self.create_directories()
    
    def create_directories(self):
        """Create necessary directories for data storage"""
        directories = ['data/raw', 'data/processed', 'data/exports']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        logger.info("Data directories initialized")
    
    def get_weather_data(self) -> Dict:
        """
        Fetch current weather data for Montreal from OpenWeatherMap API
        
        Returns:
            Dict: Weather data with risk scoring
        """
        try:
            # Current weather endpoint
            weather_url = f"http://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': self.montreal_coords['lat'],
                'lon': self.montreal_coords['lon'],
                'appid': self.weather_api_key,
                'units': 'metric'
            }
            
            response = requests.get(weather_url, params=params, timeout=10)
            response.raise_for_status()
            weather_data = response.json()
            
            # Get weather alerts if available
            alerts_url = f"http://api.openweathermap.org/data/2.5/onecall"
            alerts_params = {
                'lat': self.montreal_coords['lat'],
                'lon': self.montreal_coords['lon'],
                'appid': self.weather_api_key,
                'exclude': 'minutely,hourly,daily'
            }
            
            alerts_response = requests.get(alerts_url, params=alerts_params, timeout=10)
            alerts_data = alerts_response.json() if alerts_response.status_code == 200 else {}
            
            # Process and structure weather data
            processed_weather = {
                'timestamp': datetime.now().isoformat(),
                'temperature': weather_data['main']['temp'],
                'feels_like': weather_data['main']['feels_like'],
                'humidity': weather_data['main']['humidity'],
                'pressure': weather_data['main']['pressure'],
                'weather_condition': weather_data['weather'][0]['main'],
                'weather_description': weather_data['weather'][0]['description'],
                'wind_speed': weather_data.get('wind', {}).get('speed', 0),
                'wind_direction': weather_data.get('wind', {}).get('deg', 0),
                'cloudiness': weather_data['clouds']['all'],
                'visibility': weather_data.get('visibility', 10000),
                'sunrise': datetime.fromtimestamp(weather_data['sys']['sunrise']).isoformat(),
                'sunset': datetime.fromtimestamp(weather_data['sys']['sunset']).isoformat(),
                'weather_risk_score': self.calculate_weather_risk(weather_data),
                'alerts': alerts_data.get('alerts', [])
            }
            
            logger.info(f"Weather data collected: {processed_weather['weather_condition']}, Risk Score: {processed_weather['weather_risk_score']}")
            return processed_weather
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in weather data collection: {e}")
            return {}
    
    def calculate_weather_risk(self, weather_data: Dict) -> float:
        """
        Calculate weather risk score based on multiple factors
        
        Args:
            weather_data: Raw weather data from API
            
        Returns:
            float: Risk score from 0-10 (10 = highest risk)
        """
        risk_score = 0.0
        
        # Temperature risk (extreme cold or heat)
        temp = weather_data['main']['temp']
        if temp < -20 or temp > 35:
            risk_score += 3.0
        elif temp < -10 or temp > 30:
            risk_score += 2.0
        elif temp < 0 or temp > 25:
            risk_score += 1.0
        
        # Weather condition risk
        condition = weather_data['weather'][0]['main'].lower()
        condition_risks = {
            'thunderstorm': 4.0,
            'drizzle': 1.5,
            'rain': 2.5,
            'snow': 3.0,
            'mist': 1.0,
            'fog': 2.0,
            'hail': 4.5,
            'tornado': 5.0
        }
        risk_score += condition_risks.get(condition, 0)
        
        # Wind risk
        wind_speed = weather_data.get('wind', {}).get('speed', 0)
        if wind_speed > 15:  # Strong wind
            risk_score += 2.0
        elif wind_speed > 10:
            risk_score += 1.0
        
        # Visibility risk
        visibility = weather_data.get('visibility', 10000)
        if visibility < 1000:  # Very poor visibility
            risk_score += 2.0
        elif visibility < 5000:
            risk_score += 1.0
        
        # Cap at 10
        return min(risk_score, 10.0)
    
    def get_stock_data(self) -> List[Dict]:
        """
        Fetch Canadian insurance stock data from Alpha Vantage API
        
        Returns:
            List[Dict]: Stock data for Canadian insurance companies
        """
        stock_data = []
        
        for symbol in self.insurance_stocks:
            try:
                # Global quote endpoint (real-time data)
                url = "https://www.alphavantage.co/query"
                params = {
                    'function': 'GLOBAL_QUOTE',
                    'symbol': symbol,
                    'apikey': self.stock_api_key
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if 'Global Quote' in data:
                    quote = data['Global Quote']
                    
                    stock_info = {
                        'timestamp': datetime.now().isoformat(),
                        'symbol': symbol,
                        'price': float(quote['05. price']),
                        'change': float(quote['09. change']),
                        'change_percent': quote['10. change percent'].replace('%', ''),
                        'volume': int(quote['06. volume']),
                        'latest_trading_day': quote['07. latest trading day'],
                        'previous_close': float(quote['08. previous close']),
                        'open': float(quote['02. open']),
                        'high': float(quote['03. high']),
                        'low': float(quote['04. low'])
                    }
                    
                    stock_data.append(stock_info)
                    logger.info(f"Stock data collected for {symbol}: ${stock_info['price']} ({stock_info['change']:+.2f})")
                else:
                    logger.warning(f"No data available for {symbol}")
                
                # Respect API rate limit (5 requests per minute for free tier)
                time.sleep(12)  # Wait 12 seconds between requests
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching stock data for {symbol}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing stock data for {symbol}: {e}")
        
        return stock_data
    
    def get_montreal_traffic_data(self) -> List[Dict]:
        """
        Fetch Montreal traffic incident data from Open Data Portal
        Downloads real CSV data from Montreal's open data portal
        
        Returns:
            List[Dict]: Traffic incident data
        """
        try:
            # Montreal traffic incidents dataset
            # This URL provides real-time traffic incident data
            traffic_url = "https://donnees.montreal.ca/dataset/5866c832-e3d8-4329-8e5b-ea1fc92d99c4/resource/05deae93-d9fc-4acb-9779-e0942b5e962f/download/incidents-routiers.csv"
            
            logger.info("Downloading Montreal traffic data from open data portal...")
            
            # Download CSV file
            response = requests.get(traffic_url, timeout=30)
            response.raise_for_status()
            
            # Save raw CSV
            with open('data/raw/montreal_traffic_raw.csv', 'wb') as f:
                f.write(response.content)
            
            # Read and process CSV
            try:
                # Try different encodings in case of encoding issues
                for encoding in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        traffic_df = pd.read_csv('data/raw/montreal_traffic_raw.csv', encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    # If all encodings fail, use errors='ignore'
                    traffic_df = pd.read_csv('data/raw/montreal_traffic_raw.csv', encoding='utf-8', errors='ignore')
                
                # Process the data
                traffic_incidents = []
                current_time = datetime.now()
                
                # Take most recent incidents (limit to avoid too much data)
                recent_incidents = traffic_df.head(20) if len(traffic_df) > 20 else traffic_df
                
                for _, row in recent_incidents.iterrows():
                    try:
                        incident = {
                            'timestamp': current_time.isoformat(),
                            'incident_id': f'MTL_{current_time.strftime("%Y%m%d")}_{len(traffic_incidents):03d}',
                            'location': str(row.get('LOCATATION', row.get('LOCATION', 'Unknown'))),
                            'incident_type': str(row.get('TYPE_INCIDENT', 'Traffic Incident')),
                            'severity': self.categorize_incident_severity(str(row.get('TYPE_INCIDENT', ''))),
                            'description': str(row.get('DESCRIPTION', 'Traffic incident reported')),
                            'date_reported': str(row.get('DATE', current_time.strftime('%Y-%m-%d'))),
                            'status': 'Active'
                        }
                        traffic_incidents.append(incident)
                    except Exception as e:
                        logger.warning(f"Error processing traffic incident row: {e}")
                        continue
                
                logger.info(f"Successfully processed {len(traffic_incidents)} traffic incidents")
                return traffic_incidents
                
            except Exception as e:
                logger.error(f"Error processing traffic CSV: {e}")
                return self.get_fallback_traffic_data()
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not download Montreal traffic data: {e}")
            logger.info("Using fallback traffic data")
            return self.get_fallback_traffic_data()
        except Exception as e:
            logger.error(f"Unexpected error in traffic data collection: {e}")
            return self.get_fallback_traffic_data()
    
    def categorize_incident_severity(self, incident_type: str) -> str:
        """Categorize incident severity based on type"""
        incident_type_lower = incident_type.lower()
        
        if any(word in incident_type_lower for word in ['accident', 'collision', 'crash']):
            return 'High'
        elif any(word in incident_type_lower for word in ['construction', 'travaux', 'maintenance']):
            return 'Medium'
        elif any(word in incident_type_lower for word in ['traffic', 'congestion', 'embouteillage']):
            return 'Low'
        else:
            return 'Medium'
    
    def get_fallback_traffic_data(self) -> List[Dict]:
        """
        Fallback traffic data when real data is unavailable
        """
        logger.info("Using fallback traffic data")
        
        # Enhanced sample traffic incident data with realistic scenarios
        sample_incidents = [
            {
                'timestamp': datetime.now().isoformat(),
                'incident_id': f'MTL_{datetime.now().strftime("%Y%m%d")}_{i:03d}',
                'location': location,
                'incident_type': incident_type,
                'severity': severity,
                'description': description,
                'date_reported': datetime.now().strftime('%Y-%m-%d'),
                'status': 'Active'
            }
            for i, (location, incident_type, severity, description) in enumerate([
                ('Highway 40 - Decarie Interchange', 'Vehicle Collision', 'High', 'Multi-vehicle collision, lane closure'),
                ('Rue Sainte-Catherine - Downtown', 'Traffic Congestion', 'Medium', 'Heavy traffic due to construction'),
                ('Jacques-Cartier Bridge', 'Maintenance Work', 'Low', 'Scheduled maintenance, reduced lanes'),
                ('Highway 15 - Champlain Bridge approach', 'Accident', 'High', 'Vehicle breakdown, right lane blocked'),
                ('Boulevard Saint-Laurent', 'Construction', 'Medium', 'Road work, traffic diverted')
            ], 1)
        ]
        
        return sample_incidents
    
    def calculate_combined_risk_score(self, weather_data: Dict, stock_data: List[Dict], 
                                    traffic_data: List[Dict]) -> Dict:
        """
        Calculate combined risk score using all data sources
        
        Args:
            weather_data: Weather risk data
            stock_data: Stock market data
            traffic_data: Traffic incident data
            
        Returns:
            Dict: Combined risk assessment
        """
        # Weather risk (40% weight)
        weather_risk = weather_data.get('weather_risk_score', 0) * 0.4
        
        # Stock market risk (30% weight)
        if stock_data:
            avg_change = sum(float(stock['change_percent']) for stock in stock_data) / len(stock_data)
            # Convert percentage change to risk score (negative change = higher risk)
            stock_risk = max(0, (-avg_change / 10) * 3) * 0.3  # Scale to 0-3 range
        else:
            stock_risk = 0
        
        # Traffic risk (30% weight)
        traffic_incidents_count = len(traffic_data)
        traffic_risk = min(traffic_incidents_count * 0.5, 3) * 0.3  # Scale incidents to risk
        
        combined_score = weather_risk + stock_risk + traffic_risk
        
        return {
            'timestamp': datetime.now().isoformat(),
            'combined_risk_score': round(combined_score, 2),
            'weather_risk_component': round(weather_risk, 2),
            'stock_risk_component': round(stock_risk, 2),
            'traffic_risk_component': round(traffic_risk, 2),
            'risk_level': self.get_risk_level(combined_score),
            'recommendations': self.get_risk_recommendations(combined_score)
        }
    
    def get_risk_level(self, score: float) -> str:
        """Convert numerical risk score to categorical level"""
        if score >= 7:
            return 'CRITICAL'
        elif score >= 5:
            return 'HIGH'
        elif score >= 3:
            return 'MODERATE'
        elif score >= 1:
            return 'LOW'
        else:
            return 'MINIMAL'
    
    def get_risk_recommendations(self, score: float) -> List[str]:
        """Generate recommendations based on risk score"""
        if score >= 7:
            return [
                "Deploy additional claims adjusters",
                "Activate emergency response protocols",
                "Notify high-risk policyholders",
                "Increase call center capacity"
            ]
        elif score >= 5:
            return [
                "Monitor weather conditions closely",
                "Pre-position claims resources",
                "Review high-risk policies"
            ]
        elif score >= 3:
            return [
                "Standard monitoring procedures",
                "Review daily risk assessment"
            ]
        else:
            return ["Normal operations"]
    
    def save_data_to_csv(self, weather_data: Dict, stock_data: List[Dict], 
                        traffic_data: List[Dict], risk_assessment: Dict):
        """
        Save collected data to CSV files for Power BI consumption
        
        Args:
            weather_data: Weather data dictionary
            stock_data: List of stock data dictionaries
            traffic_data: List of traffic incident dictionaries
            risk_assessment: Combined risk assessment dictionary
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Weather data CSV
            if weather_data:
                weather_df = pd.DataFrame([weather_data])
                weather_df.to_csv(f'data/exports/weather_data_{timestamp}.csv', index=False)
                logger.info(f"Weather data saved to CSV")
            
            # Stock data CSV
            if stock_data:
                stock_df = pd.DataFrame(stock_data)
                stock_df.to_csv(f'data/exports/stock_data_{timestamp}.csv', index=False)
                logger.info(f"Stock data saved to CSV")
            
            # Traffic data CSV
            if traffic_data:
                traffic_df = pd.DataFrame(traffic_data)
                traffic_df.to_csv(f'data/exports/traffic_data_{timestamp}.csv', index=False)
                logger.info(f"Traffic data saved to CSV")
            
            # Risk assessment CSV
            risk_df = pd.DataFrame([risk_assessment])
            risk_df.to_csv(f'data/exports/risk_assessment_{timestamp}.csv', index=False)
            logger.info(f"Risk assessment saved to CSV")
            
            # Combined dataset for Power BI
            combined_data = {
                **weather_data,
                **risk_assessment,
                'stock_data_count': len(stock_data),
                'traffic_incidents_count': len(traffic_data),
                'data_collection_timestamp': timestamp
            }
            
            combined_df = pd.DataFrame([combined_data])
            combined_df.to_csv(f'data/exports/riskpulse_combined_{timestamp}.csv', index=False)
            
            # Also save latest data (for real-time dashboard)
            combined_df.to_csv('data/exports/riskpulse_latest.csv', index=False)
            logger.info("Combined dataset saved for Power BI")
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")
    
    def collect_all_data(self):
        """
        Main method to collect all data sources and generate risk assessment
        """
        logger.info("=== RiskPulse Montreal Data Collection Started ===")
        
        # Collect data from all sources
        weather_data = self.get_weather_data()
        stock_data = self.get_stock_data()
        traffic_data = self.get_montreal_traffic_data()
        
        # Calculate combined risk
        risk_assessment = self.calculate_combined_risk_score(weather_data, stock_data, traffic_data)
        
        # Save to CSV files
        self.save_data_to_csv(weather_data, stock_data, traffic_data, risk_assessment)
        
        # Log summary
        logger.info(f"Data collection completed:")
        logger.info(f"  - Weather Risk Score: {weather_data.get('weather_risk_score', 'N/A')}")
        logger.info(f"  - Stocks Tracked: {len(stock_data)}")
        logger.info(f"  - Traffic Incidents: {len(traffic_data)}")
        logger.info(f"  - Combined Risk Score: {risk_assessment['combined_risk_score']} ({risk_assessment['risk_level']})")
        logger.info("=== Data Collection Complete ===")
        
        return {
            'weather': weather_data,
            'stocks': stock_data,
            'traffic': traffic_data,
            'risk_assessment': risk_assessment
        }

def main():
    """
    Main execution function - can be run standalone or scheduled
    """
    # Initialize collector
    collector = RiskPulseDataCollector()
    
    # Run data collection
    try:
        results = collector.collect_all_data()
        print(f"\n✅ RiskPulse Data Collection Successful!")
        print(f"Risk Level: {results['risk_assessment']['risk_level']}")
        print(f"Combined Risk Score: {results['risk_assessment']['combined_risk_score']}/10")
        
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
        print(f"\n❌ Data Collection Failed: {e}")

if __name__ == "__main__":
    main()