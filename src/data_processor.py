"""
Data Processor for Counterparty Risk Analysis
Handles data loading, cleaning, and derivation of additional risk fields
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yaml
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RepoDataProcessor:
    """
    Processes repo trade data and derives additional risk fields
    """
    
    def __init__(self, config_path: str = "../config/risk_thresholds.yaml"):
        """Initialize the data processor with risk thresholds"""
        self.config = self._load_config(config_path)
        self.data = None
        self.processed_data = None
        
    def _load_config(self, config_path: str) -> Dict:
        """Load risk threshold configuration"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found. Using default thresholds.")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Return default configuration if file not found"""
        return {
            'credit_risk': {
                'rating_weights': {
                    'AAA': 0.0, 'AA': 0.2, 'A': 0.5, 'BBB': 1.0,
                    'BB': 2.0, 'B': 3.0, 'CCC': 5.0, 'CC': 10.0,
                    'C': 15.0, 'D': 100.0
                }
            },
            'market_risk': {
                'collateral': {
                    'max_volatility_pct': 15.0,
                    'min_liquidity_score': 0.7
                }
            }
        }
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load repo trade data from CSV file"""
        try:
            logger.info(f"Loading data from {file_path}")
            self.data = pd.read_csv(file_path)
            logger.info(f"Loaded {len(self.data)} records with {len(self.data.columns)} columns")
            return self.data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def clean_data(self) -> pd.DataFrame:
        """Clean and validate the data"""
        if self.data is None:
            raise ValueError("No data loaded. Call load_data() first.")
        
        logger.info("Cleaning data...")
        
        # Convert date columns
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        
        # Handle missing values
        numeric_columns = self.data.select_dtypes(include=[np.number]).columns
        self.data[numeric_columns] = self.data[numeric_columns].fillna(0)
        
        # Validate data ranges
        self._validate_data_ranges()
        
        logger.info("Data cleaning completed")
        return self.data
    
    def _validate_data_ranges(self):
        """Validate data ranges and flag outliers"""
        # Validate notional amounts
        self.data['notional_valid'] = (self.data['notional_musd'] > 0) & (self.data['notional_musd'] < 10000)
        
        # Validate haircuts
        self.data['haircut_valid'] = (self.data['haircut_pct'] >= 0) & (self.data['haircut_pct'] <= 50)
        
        # Validate repo rates
        self.data['repo_rate_valid'] = (self.data['repo_rate_pct'] >= -10) & (self.data['repo_rate_pct'] <= 50)
        
        # Log validation results
        invalid_notional = (~self.data['notional_valid']).sum()
        invalid_haircut = (~self.data['haircut_valid']).sum()
        invalid_repo_rate = (~self.data['repo_rate_valid']).sum()
        
        if invalid_notional > 0:
            logger.warning(f"Found {invalid_notional} records with invalid notional amounts")
        if invalid_haircut > 0:
            logger.warning(f"Found {invalid_haircut} records with invalid haircuts")
        if invalid_repo_rate > 0:
            logger.warning(f"Found {invalid_repo_rate} records with invalid repo rates")
    
    def derive_risk_fields(self) -> pd.DataFrame:
        """Derive additional risk fields from the base data"""
        if self.data is None:
            raise ValueError("No data loaded. Call load_data() first.")
        
        logger.info("Deriving additional risk fields...")
        
        # Create a copy for processing
        self.processed_data = self.data.copy()
        
        # 1. Credit Risk Fields
        self._derive_credit_risk_fields()
        
        # 2. Market Risk Fields
        self._derive_market_risk_fields()
        
        # 3. Liquidity Risk Fields
        self._derive_liquidity_risk_fields()
        
        # 4. Operational Risk Fields
        self._derive_operational_risk_fields()
        
        # 5. Term Risk Fields
        self._derive_term_risk_fields()
        
        # 6. Stress Testing Fields
        self._derive_stress_testing_fields()
        
        # 7. Composite Risk Score
        self._calculate_composite_risk_score()
        
        logger.info("Risk field derivation completed")
        return self.processed_data
    
    def _derive_credit_risk_fields(self):
        """Derive credit risk related fields"""
        # Rating risk weight
        rating_weights = self.config['credit_risk']['rating_weights']
        self.processed_data['rating_risk_weight'] = self.processed_data['counterparty_rating'].map(rating_weights)
        
        # Credit risk score (0-1 scale)
        self.processed_data['credit_risk_score'] = self.processed_data['rating_risk_weight'] / 100
        
        # High risk rating flag
        high_risk_threshold = self.config['credit_risk']['high_risk_rating_threshold']
        self.processed_data['high_risk_rating_flag'] = (
            self.processed_data['counterparty_rating'] >= high_risk_threshold
        )
        
        # Exposure weighted by credit risk
        self.processed_data['credit_adjusted_exposure'] = (
            self.processed_data['notional_musd'] * (1 + self.processed_data['credit_risk_score'])
        )
    
    def _derive_market_risk_fields(self):
        """Derive market risk related fields"""
        # Collateral quality score
        self.processed_data['collateral_quality_score'] = (
            (self.processed_data['collateral_liquidity_score'] * 0.4) +
            ((1 - self.processed_data['collateral_price_vol_20d_pct'] / 100) * 0.3) +
            ((self.processed_data['collateral_duration_years'] <= 5).astype(int) * 0.3)
        )
        
        # Haircut risk score
        max_haircut = self.config['market_risk']['haircut']['max_haircut_pct']
        self.processed_data['haircut_risk_score'] = self.processed_data['haircut_pct'] / max_haircut
        
        # Specialness risk score
        max_specialness = self.config['market_risk']['specialness']['max_specialness_bp']
        self.processed_data['specialness_risk_score'] = self.processed_data['specialness_bp'] / max_specialness
        
        # Market risk composite score
        self.processed_data['market_risk_score'] = (
            (1 - self.processed_data['collateral_quality_score']) * 0.4 +
            self.processed_data['haircut_risk_score'] * 0.3 +
            self.processed_data['specialness_risk_score'] * 0.3
        )
    
    def _derive_liquidity_risk_fields(self):
        """Derive liquidity risk related fields"""
        # Encumbrance risk score
        max_encumbrance = self.config['liquidity_risk']['encumbrance']['max_encumbrance_days']
        self.processed_data['encumbrance_risk_score'] = (
            self.processed_data['encumbrance_days'] / max_encumbrance
        ).clip(upper=1.0)
        
        # Margin call risk score
        max_mild_margin = self.config['liquidity_risk']['margin_calls']['max_mild_margin_call_musd']
        max_severe_margin = self.config['liquidity_risk']['margin_calls']['max_severe_margin_call_musd']
        
        self.processed_data['margin_call_risk_score'] = (
            (self.processed_data['margin_call_mild_musd'] / max_mild_margin * 0.5) +
            (self.processed_data['margin_call_severe_musd'] / max_severe_margin * 0.5)
        ).clip(upper=1.0)
        
        # Liquidity risk composite score
        self.processed_data['liquidity_risk_score'] = (
            self.processed_data['encumbrance_risk_score'] * 0.6 +
            self.processed_data['margin_call_risk_score'] * 0.4
        )
    
    def _derive_operational_risk_fields(self):
        """Derive operational risk related fields"""
        # Wrong way risk score
        self.processed_data['wrong_way_risk_score'] = self.processed_data['wrong_way_risk_flag']
        
        # Cross currency risk score
        self.processed_data['cross_currency_risk_score'] = self.processed_data['cross_ccy_flag']
        
        # CCP clearing risk score (inverse of CCP cleared flag)
        self.processed_data['ccp_clearing_risk_score'] = 1 - self.processed_data['ccp_cleared_flag']
        
        # Operational risk composite score
        self.processed_data['operational_risk_score'] = (
            self.processed_data['wrong_way_risk_score'] * 0.4 +
            self.processed_data['cross_currency_risk_score'] * 0.3 +
            self.processed_data['ccp_clearing_risk_score'] * 0.3
        )
    
    def _derive_term_risk_fields(self):
        """Derive term risk related fields"""
        # Maturity risk score
        max_term = self.config['term_risk']['maturity']['max_term_days']
        self.processed_data['maturity_risk_score'] = (
            self.processed_data['days_to_maturity'] / max_term
        ).clip(upper=1.0)
        
        # Open repo risk score
        self.processed_data['open_repo_risk_score'] = (
            self.processed_data['term_type'] == 'Open'
        ).astype(int)
        
        # Term risk composite score
        self.processed_data['term_risk_score'] = (
            self.processed_data['maturity_risk_score'] * 0.7 +
            self.processed_data['open_repo_risk_score'] * 0.3
        )
    
    def _derive_stress_testing_fields(self):
        """Derive stress testing related fields"""
        # Stress factor application
        mild_stress = self.config['stress_testing']['stress_factors']['mild_stress']
        severe_stress = self.config['stress_testing']['stress_factors']['severe_stress']
        
        # Apply stress factors to various risk metrics
        self.processed_data['mild_stress_exposure'] = (
            self.processed_data['notional_musd'] * mild_stress
        )
        self.processed_data['severe_stress_exposure'] = (
            self.processed_data['notional_musd'] * severe_stress
        )
        
        # Stress-adjusted risk scores
        self.processed_data['mild_stress_risk_score'] = (
            self.processed_data['credit_risk_score'] * mild_stress
        ).clip(upper=1.0)
        self.processed_data['severe_stress_risk_score'] = (
            self.processed_data['credit_risk_score'] * severe_stress
        ).clip(upper=1.0)
    
    def _calculate_composite_risk_score(self):
        """Calculate composite risk score"""
        weights = self.config['composite_risk']['weights']
        
        self.processed_data['composite_risk_score'] = (
            self.processed_data['credit_risk_score'] * weights['credit_risk'] +
            self.processed_data['market_risk_score'] * weights['market_risk'] +
            self.processed_data['liquidity_risk_score'] * weights['liquidity_risk'] +
            self.processed_data['operational_risk_score'] * weights['operational_risk'] +
            self.processed_data['term_risk_score'] * weights['term_risk']
        )
        
        # Risk category assignment
        thresholds = self.config['composite_risk']['score_thresholds']
        
        def assign_risk_category(score):
            if score <= thresholds['low_risk']:
                return 'Low'
            elif score <= thresholds['medium_risk']:
                return 'Medium'
            elif score <= thresholds['high_risk']:
                return 'High'
            else:
                return 'Critical'
        
        self.processed_data['risk_category'] = self.processed_data['composite_risk_score'].apply(assign_risk_category)
    
    def get_processed_data(self) -> pd.DataFrame:
        """Return the processed data"""
        if self.processed_data is None:
            raise ValueError("No processed data available. Call derive_risk_fields() first.")
        return self.processed_data
    
    def save_processed_data(self, file_path: str):
        """Save processed data to file"""
        if self.processed_data is None:
            raise ValueError("No processed data available. Call derive_risk_fields() first.")
        
        self.processed_data.to_csv(file_path, index=False)
        logger.info(f"Processed data saved to {file_path}")
    
    def get_data_summary(self) -> Dict:
        """Get summary statistics of the processed data"""
        if self.processed_data is None:
            raise ValueError("No processed data available. Call derive_risk_fields() first.")
        
        summary = {
            'total_trades': len(self.processed_data),
            'total_notional': self.processed_data['notional_musd'].sum(),
            'unique_counterparties': self.processed_data['counterparty_type'].nunique(),
            'risk_distribution': self.processed_data['risk_category'].value_counts().to_dict(),
            'average_risk_score': self.processed_data['composite_risk_score'].mean(),
            'high_risk_trades': (self.processed_data['composite_risk_score'] > 0.7).sum(),
            'critical_risk_trades': (self.processed_data['composite_risk_score'] > 0.9).sum()
        }
        
        return summary
