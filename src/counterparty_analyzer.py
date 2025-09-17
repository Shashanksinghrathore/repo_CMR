"""
Counterparty Risk Analyzer
Aggregates risk metrics at counterparty level and identifies high-risk counterparties
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class CounterpartyRiskProfile:
    """Data class for counterparty risk profile"""
    counterparty_type: str
    total_exposure: float
    trade_count: int
    average_risk_score: float
    max_risk_score: float
    risk_category: str
    credit_risk_score: float
    market_risk_score: float
    liquidity_risk_score: float
    operational_risk_score: float
    term_risk_score: float
    concentration_ratio: float
    high_risk_trades: int
    critical_risk_trades: int
    risk_flags: List[str]

class CounterpartyRiskAnalyzer:
    """
    Analyzes counterparty risk by aggregating trade-level data
    """
    
    def __init__(self, config: Dict):
        """Initialize the analyzer with configuration"""
        self.config = config
        self.counterparty_profiles = {}
        self.high_risk_counterparties = []
        self.risk_aggregates = {}
        
    def analyze_counterparties(self, data: pd.DataFrame) -> Dict:
        """
        Analyze counterparty risk profiles and identify high-risk counterparties
        
        Args:
            data: Processed trade data with risk fields
            
        Returns:
            Dictionary containing analysis results
        """
        logger.info("Starting counterparty risk analysis...")
        
        # Group by counterparty type
        counterparty_groups = data.groupby('counterparty_type')
        
        # Analyze each counterparty type
        for counterparty_type, group_data in counterparty_groups:
            profile = self._create_counterparty_profile(counterparty_type, group_data)
            self.counterparty_profiles[counterparty_type] = profile
            
            # Check if counterparty is high risk
            if self._is_high_risk_counterparty(profile):
                self.high_risk_counterparties.append(counterparty_type)
        
        # Calculate portfolio-level aggregates
        self._calculate_portfolio_aggregates(data)
        
        # Generate risk flags and recommendations
        risk_flags = self._generate_risk_flags(data)
        
        analysis_results = {
            'counterparty_profiles': self.counterparty_profiles,
            'high_risk_counterparties': self.high_risk_counterparties,
            'portfolio_aggregates': self.risk_aggregates,
            'risk_flags': risk_flags,
            'summary': self._generate_summary(data)
        }
        
        logger.info(f"Analysis completed. Found {len(self.high_risk_counterparties)} high-risk counterparties.")
        return analysis_results
    
    def _create_counterparty_profile(self, counterparty_type: str, data: pd.DataFrame) -> CounterpartyRiskProfile:
        """Create risk profile for a counterparty type"""
        
        # Basic metrics
        total_exposure = data['notional_musd'].sum()
        trade_count = len(data)
        average_risk_score = data['composite_risk_score'].mean()
        max_risk_score = data['composite_risk_score'].max()
        
        # Risk category (most common)
        risk_category = data['risk_category'].mode().iloc[0] if len(data['risk_category'].mode()) > 0 else 'Medium'
        
        # Component risk scores
        credit_risk_score = data['credit_risk_score'].mean()
        market_risk_score = data['market_risk_score'].mean()
        liquidity_risk_score = data['liquidity_risk_score'].mean()
        operational_risk_score = data['operational_risk_score'].mean()
        term_risk_score = data['term_risk_score'].mean()
        
        # Concentration ratio (percentage of total portfolio)
        total_portfolio = data['notional_musd'].sum()  # This will be recalculated at portfolio level
        concentration_ratio = (total_exposure / total_portfolio * 100) if total_portfolio > 0 else 0
        
        # High risk trade counts
        high_risk_trades = (data['composite_risk_score'] > 0.7).sum()
        critical_risk_trades = (data['composite_risk_score'] > 0.9).sum()
        
        # Generate risk flags
        risk_flags = self._generate_counterparty_risk_flags(data, total_exposure, concentration_ratio)
        
        return CounterpartyRiskProfile(
            counterparty_type=counterparty_type,
            total_exposure=total_exposure,
            trade_count=trade_count,
            average_risk_score=average_risk_score,
            max_risk_score=max_risk_score,
            risk_category=risk_category,
            credit_risk_score=credit_risk_score,
            market_risk_score=market_risk_score,
            liquidity_risk_score=liquidity_risk_score,
            operational_risk_score=operational_risk_score,
            term_risk_score=term_risk_score,
            concentration_ratio=concentration_ratio,
            high_risk_trades=high_risk_trades,
            critical_risk_trades=critical_risk_trades,
            risk_flags=risk_flags
        )
    
    def _is_high_risk_counterparty(self, profile: CounterpartyRiskProfile) -> bool:
        """Determine if a counterparty is high risk based on multiple criteria"""
        
        # Check various risk thresholds
        risk_criteria = [
            profile.average_risk_score > self.config['composite_risk']['score_thresholds']['high_risk'],
            profile.max_risk_score > self.config['composite_risk']['score_thresholds']['critical_risk'],
            profile.concentration_ratio > self.config['credit_risk']['concentration']['single_counterparty_limit_pct'],
            profile.high_risk_trades > profile.trade_count * 0.3,  # More than 30% high risk trades
            profile.critical_risk_trades > 0,  # Any critical risk trades
            profile.credit_risk_score > 0.7,
            profile.market_risk_score > 0.7,
            profile.liquidity_risk_score > 0.7
        ]
        
        # Counterparty is high risk if any 3 or more criteria are met
        return sum(risk_criteria) >= 3
    
    def _generate_counterparty_risk_flags(self, data: pd.DataFrame, total_exposure: float, concentration_ratio: float) -> List[str]:
        """Generate specific risk flags for a counterparty"""
        flags = []
        
        # Credit risk flags
        if data['high_risk_rating_flag'].any():
            flags.append("HIGH_RISK_RATING")
        
        if data['credit_risk_score'].mean() > 0.7:
            flags.append("HIGH_CREDIT_RISK")
        
        # Market risk flags
        if data['haircut_risk_score'].mean() > 0.8:
            flags.append("HIGH_HAIRCUT_RISK")
        
        if data['specialness_risk_score'].mean() > 0.8:
            flags.append("HIGH_SPECIALNESS_RISK")
        
        # Liquidity risk flags
        if data['encumbrance_risk_score'].mean() > 0.8:
            flags.append("HIGH_ENCUMBRANCE_RISK")
        
        if data['margin_call_risk_score'].mean() > 0.8:
            flags.append("HIGH_MARGIN_CALL_RISK")
        
        # Operational risk flags
        if data['wrong_way_risk_flag'].any():
            flags.append("WRONG_WAY_RISK")
        
        if data['cross_ccy_flag'].any():
            flags.append("CROSS_CURRENCY_RISK")
        
        # Concentration risk flags
        if concentration_ratio > self.config['credit_risk']['concentration']['single_counterparty_limit_pct']:
            flags.append("HIGH_CONCENTRATION")
        
        # Term risk flags
        if data['term_risk_score'].mean() > 0.8:
            flags.append("HIGH_TERM_RISK")
        
        return flags
    
    def _calculate_portfolio_aggregates(self, data: pd.DataFrame):
        """Calculate portfolio-level risk aggregates"""
        total_portfolio = data['notional_musd'].sum()
        
        self.risk_aggregates = {
            'total_portfolio_exposure': total_portfolio,
            'total_trades': len(data),
            'unique_counterparties': data['counterparty_type'].nunique(),
            'portfolio_average_risk_score': data['composite_risk_score'].mean(),
            'portfolio_max_risk_score': data['composite_risk_score'].max(),
            'high_risk_exposure': data[data['composite_risk_score'] > 0.7]['notional_musd'].sum(),
            'critical_risk_exposure': data[data['composite_risk_score'] > 0.9]['notional_musd'].sum(),
            'high_risk_exposure_pct': (data[data['composite_risk_score'] > 0.7]['notional_musd'].sum() / total_portfolio * 100) if total_portfolio > 0 else 0,
            'critical_risk_exposure_pct': (data[data['composite_risk_score'] > 0.9]['notional_musd'].sum() / total_portfolio * 100) if total_portfolio > 0 else 0,
            'risk_distribution': data['risk_category'].value_counts().to_dict(),
            'counterparty_concentration': self._calculate_concentration_metrics(data),
            'collateral_quality_distribution': data['collateral_hqla_level'].value_counts().to_dict(),
            'currency_distribution': data['currency'].value_counts().to_dict(),
            'jurisdiction_distribution': data['jurisdiction'].value_counts().to_dict()
        }
        
        # Update concentration ratios in profiles
        for profile in self.counterparty_profiles.values():
            profile.concentration_ratio = (profile.total_exposure / total_portfolio * 100) if total_portfolio > 0 else 0
    
    def _calculate_concentration_metrics(self, data: pd.DataFrame) -> Dict:
        """Calculate concentration risk metrics"""
        counterparty_exposures = data.groupby('counterparty_type')['notional_musd'].sum().sort_values(ascending=False)
        total_exposure = counterparty_exposures.sum()
        
        # Top 5 counterparty concentration
        top_5_concentration = (counterparty_exposures.head(5).sum() / total_exposure * 100) if total_exposure > 0 else 0
        
        # Herfindahl-Hirschman Index (HHI) for concentration
        hhi = ((counterparty_exposures / total_exposure) ** 2).sum() if total_exposure > 0 else 0
        
        return {
            'top_5_concentration_pct': top_5_concentration,
            'herfindahl_hirschman_index': hhi,
            'largest_counterparty_pct': (counterparty_exposures.iloc[0] / total_exposure * 100) if total_exposure > 0 else 0,
            'largest_counterparty': counterparty_exposures.index[0] if len(counterparty_exposures) > 0 else None
        }
    
    def _generate_risk_flags(self, data: pd.DataFrame) -> Dict:
        """Generate portfolio-level risk flags"""
        flags = {
            'critical_flags': [],
            'high_flags': [],
            'medium_flags': [],
            'recommendations': []
        }
        
        # Critical risk flags
        if self.risk_aggregates['critical_risk_exposure_pct'] > 10:
            flags['critical_flags'].append("CRITICAL_RISK_EXPOSURE_HIGH")
            flags['recommendations'].append("Immediately reduce exposure to critical risk trades")
        
        if self.risk_aggregates['counterparty_concentration']['herfindahl_hirschman_index'] > 0.25:
            flags['critical_flags'].append("HIGH_PORTFOLIO_CONCENTRATION")
            flags['recommendations'].append("Diversify counterparty exposure to reduce concentration risk")
        
        # High risk flags
        if self.risk_aggregates['high_risk_exposure_pct'] > 25:
            flags['high_flags'].append("HIGH_RISK_EXPOSURE_ELEVATED")
            flags['recommendations'].append("Review and potentially reduce high-risk exposures")
        
        if len(self.high_risk_counterparties) > 3:
            flags['high_flags'].append("MULTIPLE_HIGH_RISK_COUNTERPARTIES")
            flags['recommendations'].append("Review risk management policies for high-risk counterparties")
        
        # Medium risk flags
        if self.risk_aggregates['portfolio_average_risk_score'] > 0.5:
            flags['medium_flags'].append("PORTFOLIO_RISK_SCORE_ELEVATED")
            flags['recommendations'].append("Monitor portfolio risk levels and consider risk reduction strategies")
        
        if self.risk_aggregates['counterparty_concentration']['top_5_concentration_pct'] > 60:
            flags['medium_flags'].append("TOP_5_CONCENTRATION_HIGH")
            flags['recommendations'].append("Consider diversifying away from top 5 counterparties")
        
        return flags
    
    def _generate_summary(self, data: pd.DataFrame) -> Dict:
        """Generate analysis summary"""
        return {
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_counterparties': len(self.counterparty_profiles),
            'high_risk_counterparties': len(self.high_risk_counterparties),
            'portfolio_risk_level': self._determine_portfolio_risk_level(),
            'key_risk_metrics': {
                'average_risk_score': self.risk_aggregates['portfolio_average_risk_score'],
                'high_risk_exposure_pct': self.risk_aggregates['high_risk_exposure_pct'],
                'critical_risk_exposure_pct': self.risk_aggregates['critical_risk_exposure_pct'],
                'concentration_hhi': self.risk_aggregates['counterparty_concentration']['herfindahl_hirschman_index']
            }
        }
    
    def _determine_portfolio_risk_level(self) -> str:
        """Determine overall portfolio risk level"""
        risk_score = self.risk_aggregates['portfolio_average_risk_score']
        high_risk_pct = self.risk_aggregates['high_risk_exposure_pct']
        critical_risk_pct = self.risk_aggregates['critical_risk_exposure_pct']
        
        if risk_score > 0.8 or critical_risk_pct > 15:
            return 'CRITICAL'
        elif risk_score > 0.6 or high_risk_pct > 30:
            return 'HIGH'
        elif risk_score > 0.4 or high_risk_pct > 15:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_high_risk_counterparties_details(self) -> pd.DataFrame:
        """Get detailed information about high-risk counterparties"""
        if not self.high_risk_counterparties:
            return pd.DataFrame()
        
        details = []
        for counterparty in self.high_risk_counterparties:
            profile = self.counterparty_profiles[counterparty]
            details.append({
                'counterparty_type': profile.counterparty_type,
                'total_exposure': profile.total_exposure,
                'trade_count': profile.trade_count,
                'average_risk_score': profile.average_risk_score,
                'max_risk_score': profile.max_risk_score,
                'risk_category': profile.risk_category,
                'concentration_ratio': profile.concentration_ratio,
                'high_risk_trades': profile.high_risk_trades,
                'critical_risk_trades': profile.critical_risk_trades,
                'risk_flags': ', '.join(profile.risk_flags)
            })
        
        return pd.DataFrame(details)
    
    def export_analysis_results(self, file_path: str):
        """Export analysis results to Excel file"""
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            # Summary sheet
            summary_df = pd.DataFrame([self.risk_aggregates])
            summary_df.to_excel(writer, sheet_name='Portfolio_Summary', index=False)
            
            # Counterparty profiles
            profiles_df = pd.DataFrame([
                {
                    'counterparty_type': profile.counterparty_type,
                    'total_exposure': profile.total_exposure,
                    'trade_count': profile.trade_count,
                    'average_risk_score': profile.average_risk_score,
                    'max_risk_score': profile.max_risk_score,
                    'risk_category': profile.risk_category,
                    'concentration_ratio': profile.concentration_ratio,
                    'high_risk_trades': profile.high_risk_trades,
                    'critical_risk_trades': profile.critical_risk_trades,
                    'risk_flags': ', '.join(profile.risk_flags)
                }
                for profile in self.counterparty_profiles.values()
            ])
            profiles_df.to_excel(writer, sheet_name='Counterparty_Profiles', index=False)
            
            # High risk counterparties
            high_risk_df = self.get_high_risk_counterparties_details()
            if not high_risk_df.empty:
                high_risk_df.to_excel(writer, sheet_name='High_Risk_Counterparties', index=False)
        
        logger.info(f"Analysis results exported to {file_path}")
