#!/usr/bin/env python3
"""
Test script to demonstrate high-risk counterparty scenarios
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime

# Add src directory to path
sys.path.append('src')

from data_processor import RepoDataProcessor
from counterparty_analyzer import CounterpartyRiskAnalyzer

def create_high_risk_test_data():
    """Create test data with high-risk scenarios"""
    
    # Load original data
    processor = RepoDataProcessor('config/risk_thresholds.yaml')
    processor.load_data('data/repo_simulation_with_cash_legs.csv')
    processor.clean_data()
    original_data = processor.data.copy()
    
    # Create high-risk scenarios
    high_risk_data = original_data.copy()
    
    # Scenario 1: High-risk HedgeFund with poor ratings and high exposure
    hedgefund_mask = high_risk_data['counterparty_type'] == 'HedgeFund'
    high_risk_data.loc[hedgefund_mask, 'counterparty_rating'] = 'BB'  # Below investment grade
    high_risk_data.loc[hedgefund_mask, 'haircut_pct'] = 25.0  # High haircut
    high_risk_data.loc[hedgefund_mask, 'specialness_bp'] = 75  # High specialness
    high_risk_data.loc[hedgefund_mask, 'wrong_way_risk_flag'] = 1  # Wrong way risk
    high_risk_data.loc[hedgefund_mask, 'notional_musd'] *= 2  # Double exposure
    
    # Scenario 2: High-risk Dealer with concentration issues
    dealer_mask = high_risk_data['counterparty_type'] == 'Dealer'
    high_risk_data.loc[dealer_mask, 'counterparty_rating'] = 'BBB'  # Lower rating
    high_risk_data.loc[dealer_mask, 'encumbrance_days'] = 45  # Long encumbrance
    high_risk_data.loc[dealer_mask, 'margin_call_severe_musd'] = 15.0  # High margin call risk
    high_risk_data.loc[dealer_mask, 'notional_musd'] *= 1.5  # Increase exposure
    
    # Scenario 3: High-risk Bank with operational issues
    bank_mask = high_risk_data['counterparty_type'] == 'Bank'
    high_risk_data.loc[bank_mask, 'cross_ccy_flag'] = 1  # Cross currency risk
    high_risk_data.loc[bank_mask, 'ccp_cleared_flag'] = 0  # Not CCP cleared
    high_risk_data.loc[bank_mask, 'days_to_maturity'] = 400  # Long term
    high_risk_data.loc[bank_mask, 'notional_musd'] *= 1.8  # High exposure
    
    return high_risk_data

def test_high_risk_scenarios():
    """Test the system with high-risk scenarios"""
    
    print("🧪 Testing High-Risk Counterparty Scenarios")
    print("=" * 50)
    
    # Create test data
    test_data = create_high_risk_test_data()
    
    # Process the data
    processor = RepoDataProcessor('config/risk_thresholds.yaml')
    processor.data = test_data
    processed_data = processor.derive_risk_fields()
    
    # Analyze counterparties
    analyzer = CounterpartyRiskAnalyzer(processor.config)
    analysis_results = analyzer.analyze_counterparties(processed_data)
    
    # Display results
    print(f"\n📊 ANALYSIS RESULTS:")
    print(f"Total Counterparties: {analysis_results['summary']['total_counterparties']}")
    print(f"High-Risk Counterparties: {analysis_results['summary']['high_risk_counterparties']}")
    print(f"Portfolio Risk Level: {analysis_results['summary']['portfolio_risk_level']}")
    
    # Show high-risk counterparties
    if analysis_results['high_risk_counterparties']:
        print(f"\n🔴 HIGH-RISK COUNTERPARTIES IDENTIFIED:")
        for counterparty in analysis_results['high_risk_counterparties']:
            profile = analysis_results['counterparty_profiles'][counterparty]
            print(f"\n  • {counterparty}:")
            print(f"    - Total Exposure: ${profile.total_exposure:,.2f}M")
            print(f"    - Average Risk Score: {profile.average_risk_score:.3f}")
            print(f"    - Risk Category: {profile.risk_category}")
            print(f"    - Concentration Ratio: {profile.concentration_ratio:.2f}%")
            print(f"    - High Risk Trades: {profile.high_risk_trades}")
            print(f"    - Critical Risk Trades: {profile.critical_risk_trades}")
            print(f"    - Risk Flags: {', '.join(profile.risk_flags)}")
    else:
        print("\n✅ No high-risk counterparties identified in test scenario.")
    
    # Show risk flags
    risk_flags = analysis_results['risk_flags']
    if risk_flags['critical_flags'] or risk_flags['high_flags']:
        print(f"\n🚨 RISK FLAGS:")
        if risk_flags['critical_flags']:
            print(f"  Critical: {', '.join(risk_flags['critical_flags'])}")
        if risk_flags['high_flags']:
            print(f"  High: {', '.join(risk_flags['high_flags'])}")
    
    # Show recommendations
    if risk_flags['recommendations']:
        print(f"\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(risk_flags['recommendations'], 1):
            print(f"  {i}. {rec}")
    
    # Save test results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"reports/test_scenario_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save processed test data
    processed_data.to_csv(f"{output_dir}/test_processed_data.csv", index=False)
    
    # Export analysis results
    analyzer.export_analysis_results(f"{output_dir}/test_analysis_results.xlsx")
    
    print(f"\n✅ Test results saved to: {output_dir}")
    
    return analysis_results

def compare_scenarios():
    """Compare original vs high-risk scenarios"""
    
    print("\n📈 COMPARING ORIGINAL VS HIGH-RISK SCENARIOS")
    print("=" * 60)
    
    # Original scenario
    processor_orig = RepoDataProcessor('config/risk_thresholds.yaml')
    processor_orig.load_data('data/repo_simulation_with_cash_legs.csv')
    processor_orig.clean_data()
    orig_processed = processor_orig.derive_risk_fields()
    
    analyzer_orig = CounterpartyRiskAnalyzer(processor_orig.config)
    orig_results = analyzer_orig.analyze_counterparties(orig_processed)
    
    # High-risk scenario
    test_data = create_high_risk_test_data()
    processor_test = RepoDataProcessor('config/risk_thresholds.yaml')
    processor_test.data = test_data
    test_processed = processor_test.derive_risk_fields()
    
    analyzer_test = CounterpartyRiskAnalyzer(processor_test.config)
    test_results = analyzer_test.analyze_counterparties(test_processed)
    
    # Comparison table
    print(f"\n{'Metric':<30} {'Original':<15} {'High-Risk':<15} {'Change':<10}")
    print("-" * 70)
    
    metrics = [
        ('Portfolio Risk Level', orig_results['summary']['portfolio_risk_level'], test_results['summary']['portfolio_risk_level']),
        ('Average Risk Score', f"{orig_results['summary']['key_risk_metrics']['average_risk_score']:.3f}", f"{test_results['summary']['key_risk_metrics']['average_risk_score']:.3f}"),
        ('High Risk Exposure %', f"{orig_results['summary']['key_risk_metrics']['high_risk_exposure_pct']:.2f}%", f"{test_results['summary']['key_risk_metrics']['high_risk_exposure_pct']:.2f}%"),
        ('Critical Risk Exposure %', f"{orig_results['summary']['key_risk_metrics']['critical_risk_exposure_pct']:.2f}%", f"{test_results['summary']['key_risk_metrics']['critical_risk_exposure_pct']:.2f}%"),
        ('High-Risk Counterparties', orig_results['summary']['high_risk_counterparties'], test_results['summary']['high_risk_counterparties']),
        ('Concentration HHI', f"{orig_results['summary']['key_risk_metrics']['concentration_hhi']:.3f}", f"{test_results['summary']['key_risk_metrics']['concentration_hhi']:.3f}")
    ]
    
    for metric, orig_val, test_val in metrics:
        change = "↗️" if orig_val != test_val else "➡️"
        print(f"{metric:<30} {orig_val:<15} {test_val:<15} {change:<10}")

if __name__ == "__main__":
    # Run high-risk scenario test
    test_results = test_high_risk_scenarios()
    
    # Compare scenarios
    compare_scenarios()
    
    print(f"\n🎯 Test completed successfully!")
