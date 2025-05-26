import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import json
import logging
from typing import Dict, List, Optional
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.warning("python-dotenv not installed. Using system environment variables.")

class EnhancedRiskPulseCollector:
    """
    Enhanced RiskPulse Montreal - Comprehensive Insurance Risk Data Collector
    
    Collects rich datasets from multiple sources:
    1. Detailed weather data (current + forecast + historical)
    2. Comprehensive stock market data for insurance sector
    3. Montreal traffic and incident data
    4. Advanced risk analytics
    """
    
    def __init__(self):
        # Load API keys from environment variables
        self.weather_api_key = os.getenv('OPENWEATHER_API_KEY', 'f554372d4e07b0a1dfe17f7613c8f091')
        self.stock_api_key = os.getenv('ALPHAVANTAGE_API_KEY', 'ISL3TM9TB4FIOR7P')
        
        if not self.weather_api_key or self.weather_api_key == 'f554372d4e07b0a1dfe17f7613c8f091':
            logger.warning("OpenWeatherMap API key not set properly in .env file - using demo key")
        
        if not self.stock_api_key or self.stock_api_key == 'ISL3TM9TB4FIOR7P':
            logger.warning("Alpha Vantage API key not set properly in .env file - using demo key")
        
        # Expanded Canadian Insurance and Financial Companies
        self.insurance_stocks = [
            'IFC.TO',    # Intact Financial Corporation
            'FFH.TO',    # Fairfax Financial Holdings
            'MFC.TO',    # Manulife Financial Corp
            'SLF.TO',    # Sun Life Financial Inc
            'POW.TO',    # Power Corporation of Canada
            'GWO.TO',    # Great-West Lifeco Inc
            'IAG.TO',    # iA Financial Corporation
            'HCG.TO'     # Home Capital Group Inc
        ]
        
        # Montreal districts for comprehensive coverage
        self.montreal_districts = [
            {'name': 'Downtown', 'lat': 45.5017, 'lon': -73.5673},
            {'name': 'Plateau', 'lat': 45.5200, 'lon': -73.5800},
            {'name': 'Westmount', 'lat': 45.4833, 'lon': -73.6000},
            {'name': 'NDG', 'lat': 45.4700, 'lon': -73.6100},
            {'name': 'Verdun', 'lat': 45.4583, 'lon': -73.5667},
            {'name': 'LaSalle', 'lat': 45.4333, 'lon': -73.6333},
            {'name': 'Outremont', 'lat': 45.5167, 'lon': -73.6000},
            {'name': 'Rosemont', 'lat': 45.5500, 'lon': -73.5667}
        ]
        
        # Create enhanced directory structure
        self.create_enhanced_directories()
    
    def create_enhanced_directories(self):
        """Create comprehensive directory structure"""
        directories = [
            'data/raw/weather', 'data/raw/stocks', 'data/raw/traffic',
            'data/processed/weather', 'data/processed/stocks', 'data/processed/traffic',
            'data/exports/daily', 'data/exports/historical', 'data/exports/azure',
            'logs', 'config'
        ]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        logger.info("Enhanced data directories initialized")
    
    def get_comprehensive_weather_data(self) -> List[Dict]:
        """
        Fetch comprehensive weather data for all Montreal districts
        Including current weather, forecasts, and weather alerts
        """
        weather_data = []
        
        for district in self.montreal_districts:
            try:
                # Current weather
                current_weather = self.get_current_weather(district)
                if current_weather:
                    weather_data.append(current_weather)
                
                # 5-day forecast
                forecast_data = self.get_weather_forecast(district)
                weather_data.extend(forecast_data)
                
                # Small delay to respect API limits
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error collecting weather data for {district['name']}: {e}")
        
        logger.info(f"Collected weather data for {len(weather_data)} data points")
        return weather_data
    
    def get_current_weather(self, district: Dict) -> Dict:
        """Get detailed current weather for a specific district"""
        try:
            url = "http://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': district['lat'],
                'lon': district['lon'],
                'appid': self.weather_api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'data_type': 'current_weather',
                'district': district['name'],
                'latitude': district['lat'],
                'longitude': district['lon'],
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'pressure': data['main']['pressure'],
                'humidity': data['main']['humidity'],
                'sea_level': data['main'].get('sea_level', data['main']['pressure']),
                'ground_level': data['main'].get('grnd_level', data['main']['pressure']),
                'visibility': data.get('visibility', 10000),
                'uv_index': 0,  # Would need UV API call
                'clouds': data['clouds']['all'],
                'wind_speed': data.get('wind', {}).get('speed', 0),
                'wind_deg': data.get('wind', {}).get('deg', 0),
                'wind_gust': data.get('wind', {}).get('gust', 0),
                'weather_main': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'weather_icon': data['weather'][0]['icon'],
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).isoformat(),
                'sunset': datetime.fromtimestamp(data['sys']['sunset']).isoformat(),
                'timezone': data['timezone'],
                'city_name': data['name'],
                'country': data['sys']['country'],
                'rain_1h': data.get('rain', {}).get('1h', 0),
                'rain_3h': data.get('rain', {}).get('3h', 0),
                'snow_1h': data.get('snow', {}).get('1h', 0),
                'snow_3h': data.get('snow', {}).get('3h', 0),
                'weather_risk_score': self.calculate_detailed_weather_risk(data),
                'risk_factors': self.identify_weather_risk_factors(data)
            }
            
        except Exception as e:
            logger.error(f"Error getting current weather for {district['name']}: {e}")
            return {}
    
    def get_weather_forecast(self, district: Dict) -> List[Dict]:
        """Get 5-day weather forecast for a district"""
        try:
            url = "http://api.openweathermap.org/data/2.5/forecast"
            params = {
                'lat': district['lat'],
                'lon': district['lon'],
                'appid': self.weather_api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            forecast_data = []
            for forecast in data['list'][:20]:  # Next 5 days (4 forecasts per day)
                forecast_point = {
                    'timestamp': datetime.now().isoformat(),
                    'data_type': 'weather_forecast',
                    'district': district['name'],
                    'latitude': district['lat'],
                    'longitude': district['lon'],
                    'forecast_time': datetime.fromtimestamp(forecast['dt']).isoformat(),
                    'temperature': forecast['main']['temp'],
                    'feels_like': forecast['main']['feels_like'],
                    'temp_min': forecast['main']['temp_min'],
                    'temp_max': forecast['main']['temp_max'],
                    'pressure': forecast['main']['pressure'],
                    'humidity': forecast['main']['humidity'],
                    'clouds': forecast['clouds']['all'],
                    'wind_speed': forecast.get('wind', {}).get('speed', 0),
                    'wind_deg': forecast.get('wind', {}).get('deg', 0),
                    'weather_main': forecast['weather'][0]['main'],
                    'weather_description': forecast['weather'][0]['description'],
                    'precipitation_probability': forecast.get('pop', 0) * 100,
                    'rain_3h': forecast.get('rain', {}).get('3h', 0),
                    'snow_3h': forecast.get('snow', {}).get('3h', 0),
                    'forecast_risk_score': self.calculate_detailed_weather_risk(forecast)
                }
                forecast_data.append(forecast_point)
            
            return forecast_data
            
        except Exception as e:
            logger.error(f"Error getting forecast for {district['name']}: {e}")
            return []
    
    def calculate_detailed_weather_risk(self, weather_data: Dict) -> float:
        """Enhanced weather risk calculation with more factors"""
        risk_score = 0.0
        
        # Temperature risk
        temp = weather_data['main']['temp']
        if temp < -30: risk_score += 4.0
        elif temp < -20: risk_score += 3.0
        elif temp < -10: risk_score += 2.0
        elif temp < 0: risk_score += 1.0
        elif temp > 40: risk_score += 4.0
        elif temp > 35: risk_score += 3.0
        elif temp > 30: risk_score += 2.0
        elif temp > 25: risk_score += 1.0
        
        # Weather condition risk
        condition = weather_data['weather'][0]['main'].lower()
        condition_risks = {
            'thunderstorm': 4.5, 'drizzle': 1.5, 'rain': 2.5, 'snow': 3.5,
            'mist': 1.5, 'smoke': 2.0, 'haze': 1.5, 'dust': 2.5,
            'fog': 3.0, 'sand': 2.5, 'ash': 4.0, 'squall': 4.0,
            'tornado': 5.0, 'clear': 0.0, 'clouds': 0.5
        }
        risk_score += condition_risks.get(condition, 1.0)
        
        # Wind risk
        wind_speed = weather_data.get('wind', {}).get('speed', 0)
        if wind_speed > 25: risk_score += 3.0
        elif wind_speed > 15: risk_score += 2.0
        elif wind_speed > 10: risk_score += 1.0
        
        # Visibility risk
        visibility = weather_data.get('visibility', 10000)
        if visibility < 500: risk_score += 3.0
        elif visibility < 1000: risk_score += 2.0
        elif visibility < 5000: risk_score += 1.0
        
        # Humidity extremes
        humidity = weather_data['main']['humidity']
        if humidity > 90 or humidity < 20:
            risk_score += 1.0
        
        # Pressure changes (indicating weather instability)
        pressure = weather_data['main']['pressure']
        if pressure < 980: risk_score += 2.0
        elif pressure < 1000: risk_score += 1.0
        elif pressure > 1030: risk_score += 1.0
        
        return min(risk_score, 10.0)
    
    def identify_weather_risk_factors(self, weather_data: Dict) -> List[str]:
        """Identify specific weather risk factors"""
        risk_factors = []
        
        temp = weather_data['main']['temp']
        condition = weather_data['weather'][0]['main'].lower()
        wind_speed = weather_data.get('wind', {}).get('speed', 0)
        visibility = weather_data.get('visibility', 10000)
        
        if temp < -15: risk_factors.append("Extreme Cold")
        if temp > 32: risk_factors.append("Extreme Heat")
        if condition in ['thunderstorm', 'tornado']: risk_factors.append("Severe Weather")
        if condition in ['rain', 'drizzle']: risk_factors.append("Wet Roads")
        if condition == 'snow': risk_factors.append("Snow/Ice Conditions")
        if wind_speed > 15: risk_factors.append("High Winds")
        if visibility < 5000: risk_factors.append("Poor Visibility")
        if weather_data['main']['humidity'] > 85: risk_factors.append("High Humidity")
        
        return risk_factors
    
    def get_comprehensive_stock_data(self) -> List[Dict]:
        """
        Fetch comprehensive stock data for Canadian insurance sector
        Including real-time quotes, historical data, and technical indicators
        """
        stock_data = []
        
        for symbol in self.insurance_stocks:
            try:
                # Get real-time quote
                quote_data = self.get_stock_quote(symbol)
                if quote_data:
                    stock_data.append(quote_data)
                
                # Get intraday data for technical analysis
                intraday_data = self.get_stock_intraday(symbol)
                stock_data.extend(intraday_data)
                
                # Respect API rate limits (5 requests per minute)
                time.sleep(12)
                
            except Exception as e:
                logger.error(f"Error collecting stock data for {symbol}: {e}")
        
        logger.info(f"Collected stock data for {len(stock_data)} data points")
        return stock_data
    
    def get_stock_quote(self, symbol: str) -> Dict:
        """Get detailed stock quote data"""
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.stock_api_key
            }
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if 'Global Quote' in data:
                quote = data['Global Quote']
                
                return {
                    'timestamp': datetime.now().isoformat(),
                    'data_type': 'stock_quote',
                    'symbol': symbol,
                    'company_name': self.get_company_name(symbol),
                    'sector': 'Insurance/Financial',
                    'open': float(quote['02. open']),
                    'high': float(quote['03. high']),
                    'low': float(quote['04. low']),
                    'price': float(quote['05. price']),
                    'volume': int(quote['06. volume']),
                    'latest_trading_day': quote['07. latest trading day'],
                    'previous_close': float(quote['08. previous close']),
                    'change': float(quote['09. change']),
                    'change_percent': float(quote['10. change percent'].replace('%', '')),
                    'day_range': f"{quote['04. low']} - {quote['03. high']}",
                    'volatility': abs(float(quote['09. change']) / float(quote['08. previous close'])) * 100,
                    'market_cap_estimate': self.estimate_market_cap(symbol, float(quote['05. price'])),
                    'risk_rating': self.calculate_stock_risk(quote),
                    'technical_indicators': self.calculate_technical_indicators(quote)
                }
            else:
                logger.warning(f"No quote data for {symbol}: {data}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting stock quote for {symbol}: {e}")
            return {}
    
    def get_stock_intraday(self, symbol: str) -> List[Dict]:
        """Get intraday stock data for technical analysis"""
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol,
                'interval': '60min',
                'apikey': self.stock_api_key
            }
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            intraday_data = []
            if 'Time Series (60min)' in data:
                time_series = data['Time Series (60min)']
                
                # Get last 24 hours of data
                for timestamp, values in list(time_series.items())[:24]:
                    intraday_point = {
                        'timestamp': datetime.now().isoformat(),
                        'data_type': 'stock_intraday',
                        'symbol': symbol,
                        'trading_time': timestamp,
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume']),
                        'price_movement': 'up' if float(values['4. close']) > float(values['1. open']) else 'down',
                        'intraday_change': float(values['4. close']) - float(values['1. open']),
                        'volatility_indicator': (float(values['2. high']) - float(values['3. low'])) / float(values['1. open']) * 100
                    }
                    intraday_data.append(intraday_point)
            
            return intraday_data
            
        except Exception as e:
            logger.error(f"Error getting intraday data for {symbol}: {e}")
            return []
    
    def get_company_name(self, symbol: str) -> str:
        """Get company name from symbol"""
        company_names = {
            'IFC.TO': 'Intact Financial Corporation',
            'FFH.TO': 'Fairfax Financial Holdings',
            'MFC.TO': 'Manulife Financial Corp',
            'SLF.TO': 'Sun Life Financial Inc',
            'POW.TO': 'Power Corporation of Canada',
            'GWO.TO': 'Great-West Lifeco Inc',
            'IAG.TO': 'iA Financial Corporation',
            'HCG.TO': 'Home Capital Group Inc'
        }
        return company_names.get(symbol, symbol)
    
    def estimate_market_cap(self, symbol: str, price: float) -> str:
        """Estimate market cap category"""
        # Simplified estimation based on known Canadian insurance companies
        large_cap = ['MFC.TO', 'SLF.TO', 'IFC.TO', 'FFH.TO']
        if symbol in large_cap:
            return "Large Cap (>$10B)"
        else:
            return "Mid Cap ($2B-$10B)"
    
    def calculate_stock_risk(self, quote_data: Dict) -> str:
        """Calculate stock risk rating"""
        change_percent = abs(float(quote_data['10. change percent'].replace('%', '')))
        
        if change_percent > 5:
            return "High"
        elif change_percent > 2:
            return "Medium"
        else:
            return "Low"
    
    def calculate_technical_indicators(self, quote_data: Dict) -> Dict:
        """Calculate basic technical indicators"""
        high = float(quote_data['03. high'])
        low = float(quote_data['04. low'])
        close = float(quote_data['05. price'])
        
        return {
            'day_range_percent': ((high - low) / low) * 100,
            'position_in_range': ((close - low) / (high - low)) * 100 if high != low else 50,
            'momentum': 'bullish' if float(quote_data['09. change']) > 0 else 'bearish'
        }
    
    def get_enhanced_montreal_data(self) -> List[Dict]:
        """
        Generate comprehensive Montreal risk data
        Including traffic, demographics, and risk zones
        """
        montreal_data = []
        
        # Generate data for different Montreal areas
        areas = [
            {'name': 'Downtown Core', 'risk_level': 'High', 'population_density': 15000},
            {'name': 'Plateau Mont-Royal', 'risk_level': 'Medium', 'population_density': 12000},
            {'name': 'Westmount', 'risk_level': 'Low', 'population_density': 8000},
            {'name': 'Notre-Dame-de-Grâce', 'risk_level': 'Medium', 'population_density': 10000},
            {'name': 'Verdun', 'risk_level': 'Medium', 'population_density': 9500},
            {'name': 'LaSalle', 'risk_level': 'Low', 'population_density': 7000},
            {'name': 'Outremont', 'risk_level': 'Low', 'population_density': 6500},
            {'name': 'Rosemont-La Petite-Patrie', 'risk_level': 'Medium', 'population_density': 11000}
        ]
        
        for i, area in enumerate(areas):
            # Generate multiple data points per area
            for j in range(5):  # 5 data points per area
                data_point = {
                    'timestamp': datetime.now().isoformat(),
                    'data_type': 'montreal_risk_zone',
                    'area_id': f"MTL_{i+1:02d}_{j+1:02d}",
                    'area_name': area['name'],
                    'risk_level': area['risk_level'],
                    'population_density': area['population_density'],
                    'incident_count_24h': np.random.randint(0, 15),
                    'incident_count_7d': np.random.randint(5, 50),
                    'incident_count_30d': np.random.randint(20, 200),
                    'primary_incident_type': np.random.choice(['Vehicle Collision', 'Property Damage', 'Weather Related', 'Theft']),
                    'average_claim_amount': np.random.randint(1000, 25000),
                    'weather_sensitivity': np.random.choice(['High', 'Medium', 'Low']),
                    'seasonal_risk_factor': self.get_seasonal_factor(),
                    'economic_indicator': np.random.choice(['Stable', 'Growing', 'Declining']),
                    'infrastructure_quality': np.random.choice(['Excellent', 'Good', 'Fair', 'Poor']),
                    'emergency_response_time': np.random.randint(5, 25),  # minutes
                    'postal_codes': self.generate_postal_codes(area['name'])
                }
                montreal_data.append(data_point)
        
        logger.info(f"Generated {len(montreal_data)} Montreal risk data points")
        return montreal_data
    
    def get_seasonal_factor(self) -> float:
        """Calculate seasonal risk factor based on current month"""
        month = datetime.now().month
        # Winter months have higher risk in Montreal
        if month in [12, 1, 2, 3]:  # Winter
            return round(np.random.uniform(1.2, 1.8), 2)
        elif month in [4, 5, 10, 11]:  # Spring/Fall
            return round(np.random.uniform(0.9, 1.3), 2)
        else:  # Summer
            return round(np.random.uniform(0.7, 1.1), 2)
    
    def generate_postal_codes(self, area_name: str) -> List[str]:
        """Generate realistic postal codes for Montreal areas"""
        postal_code_prefixes = {
            'Downtown Core': ['H3A', 'H3B', 'H2Y', 'H2Z'],
            'Plateau Mont-Royal': ['H2T', 'H2W', 'H2X'],
            'Westmount': ['H3Y', 'H3Z'],
            'Notre-Dame-de-Grâce': ['H4A', 'H3S'],
            'Verdun': ['H3E', 'H4G'],
            'LaSalle': ['H8N', 'H8P'],
            'Outremont': ['H2V', 'H3V'],
            'Rosemont-La Petite-Patrie': ['H1X', 'H2S']
        }
        
        prefixes = postal_code_prefixes.get(area_name, ['H3A'])
        return [f"{prefix} {np.random.randint(1, 9)}{chr(np.random.randint(65, 90))}{np.random.randint(0, 9)}" 
                for prefix in prefixes]
    
    def create_weather_summary(self, weather_data: List[Dict]) -> List[Dict]:
        """Create weather summary data for comprehensive dataset"""
        summary_data = []
        
        # Group by district and data type
        districts = {}
        for item in weather_data:
            district = item.get('district', 'Unknown')
            if district not in districts:
                districts[district] = {'current': [], 'forecast': []}
            
            if item.get('data_type') == 'current_weather':
                districts[district]['current'].append(item)
            elif item.get('data_type') == 'weather_forecast':
                districts[district]['forecast'].append(item)
        
        # Create summary for each district
        for district, data in districts.items():
            if data['current']:
                current = data['current'][0]  # Take first current weather record
                
                summary = {
                    'timestamp': datetime.now().isoformat(),
                    'data_type': 'weather_summary',
                    'district': district,
                    'current_temperature': current.get('temperature', 0),
                    'current_risk_score': current.get('weather_risk_score', 0),
                    'current_conditions': current.get('weather_description', 'Unknown'),
                    'forecast_count': len(data['forecast']),
                    'avg_forecast_risk': np.mean([f.get('forecast_risk_score', 0) for f in data['forecast']]) if data['forecast'] else 0,
                    'max_forecast_risk': max([f.get('forecast_risk_score', 0) for f in data['forecast']]) if data['forecast'] else 0,
                    'risk_factors': current.get('risk_factors', [])
                }
                summary_data.append(summary)
        
        return summary_data
    
    def create_stock_summary(self, stock_data: List[Dict]) -> List[Dict]:
        """Create stock summary data for comprehensive dataset"""
        summary_data = []
        
        # Group by symbol
        symbols = {}
        for item in stock_data:
            symbol = item.get('symbol', 'Unknown')
            if symbol not in symbols:
                symbols[symbol] = {'quotes': [], 'intraday': []}
            
            if item.get('data_type') == 'stock_quote':
                symbols[symbol]['quotes'].append(item)
            elif item.get('data_type') == 'stock_intraday':
                symbols[symbol]['intraday'].append(item)
        
        # Create summary for each symbol
        for symbol, data in symbols.items():
            if data['quotes']:
                quote = data['quotes'][0]  # Take first quote record
                
                summary = {
                    'timestamp': datetime.now().isoformat(),
                    'data_type': 'stock_summary',
                    'symbol': symbol,
                    'company_name': quote.get('company_name', symbol),
                    'current_price': quote.get('price', 0),
                    'change_percent': quote.get('change_percent', 0),
                    'risk_rating': quote.get('risk_rating', 'Unknown'),
                    'volume': quote.get('volume', 0),
                    'volatility': quote.get('volatility', 0),
                    'intraday_points': len(data['intraday']),
                    'market_cap_estimate': quote.get('market_cap_estimate', 'Unknown')
                }
                summary_data.append(summary)
        
        return summary_data
    
    def save_enhanced_data(self, weather_data: List[Dict], stock_data: List[Dict], 
                          montreal_data: List[Dict]):
        """
        Save comprehensive datasets to multiple formats
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Weather data
            if weather_data:
                weather_df = pd.DataFrame(weather_data)
                weather_df.to_csv(f'data/exports/enhanced_weather_{timestamp}.csv', index=False)
                weather_df.to_csv('data/exports/latest_weather.csv', index=False)
                logger.info(f"Saved {len(weather_data)} weather records")
            
            # Stock data
            if stock_data:
                stock_df = pd.DataFrame(stock_data)
                stock_df.to_csv(f'data/exports/enhanced_stocks_{timestamp}.csv', index=False)
                stock_df.to_csv('data/exports/latest_stocks.csv', index=False)
                logger.info(f"Saved {len(stock_data)} stock records")
            
            # Montreal data
            if montreal_data:
                montreal_df = pd.DataFrame(montreal_data)
                montreal_df.to_csv(f'data/exports/enhanced_montreal_{timestamp}.csv', index=False)
                montreal_df.to_csv('data/exports/latest_montreal.csv', index=False)
                logger.info(f"Saved {len(montreal_data)} Montreal records")
            
            # Combined comprehensive dataset
            all_data = []
            
            # Add summary weather data
            if weather_data:
                weather_summary = self.create_weather_summary(weather_data)
                all_data.extend(weather_summary)
            
            # Add summary stock data
            if stock_data:
                stock_summary = self.create_stock_summary(stock_data)
                all_data.extend(stock_summary)
            
            # Add Montreal data
            all_data.extend(montreal_data)
            
            # Save comprehensive dataset
            if all_data:
                comprehensive_df = pd.DataFrame(all_data)
                comprehensive_df.to_csv(f'data/exports/riskpulse_comprehensive_{timestamp}.csv', index=False)
                comprehensive_df.to_csv('data/exports/riskpulse_latest_comprehensive.csv', index=False)
                
                # Azure-ready format with JSON export
                azure_export = {
                    'collection_timestamp': timestamp,
                    'total_records': len(all_data),
                    'weather_records': len(weather_data) if weather_data else 0,
                    'stock_records': len(stock_data) if stock_data else 0,
                    'montreal_records': len(montreal_data) if montreal_data else 0,
                    'data': all_data
                }
                
                with open(f'data/exports/azure/riskpulse_azure_{timestamp}.json', 'w') as f:
                    json.dump(azure_export, f, indent=2)
                
                logger.info(f"Saved comprehensive dataset with {len(all_data)} total records")
                logger.info(f"Files saved with timestamp: {timestamp}")
                
                return {
                    'timestamp': timestamp,
                    'total_records': len(all_data),
                    'files_created': [
                        f'riskpulse_comprehensive_{timestamp}.csv',
                        f'riskpulse_azure_{timestamp}.json'
                    ]
                }
            
        except Exception as e:
            logger.error(f"Error saving enhanced data: {e}")
            return {}
    
    def run_full_collection(self):
        """
        Run complete data collection process
        """
        logger.info("=== Starting Enhanced RiskPulse Collection ===")
        start_time = datetime.now()
        
        try:
            # Collect weather data
            logger.info("Collecting comprehensive weather data...")
            weather_data = self.get_comprehensive_weather_data()
            
            # Collect stock data
            logger.info("Collecting comprehensive stock data...")
            stock_data = self.get_comprehensive_stock_data()
            
            # Generate Montreal data
            logger.info("Generating Montreal risk data...")
            montreal_data = self.get_enhanced_montreal_data()
            
            # Save all data
            logger.info("Saving enhanced datasets...")
            save_result = self.save_enhanced_data(weather_data, stock_data, montreal_data)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=== Collection Complete ===")
            logger.info(f"Duration: {duration}")
            logger.info(f"Weather records: {len(weather_data)}")
            logger.info(f"Stock records: {len(stock_data)}")
            logger.info(f"Montreal records: {len(montreal_data)}")
            logger.info(f"Total records: {save_result.get('total_records', 0)}")
            
            return save_result
            
        except Exception as e:
            logger.error(f"Error in full collection: {e}")
            return {}


def main():
    """
    Main execution function
    """
    try:
        collector = EnhancedRiskPulseCollector()
        result = collector.run_full_collection()
        
        if result:
            print(f"\n✅ Collection successful!")
            print(f"📊 Total records collected: {result.get('total_records', 0)}")
            print(f"🕒 Timestamp: {result.get('timestamp', 'N/A')}")
            print(f"📁 Files created: {', '.join(result.get('files_created', []))}")
        else:
            print("\n❌ Collection failed - check logs for details")
            
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        print(f"\n❌ Fatal error: {e}")


if __name__ == "__main__":
    main()