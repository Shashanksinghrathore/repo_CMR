
"""
Main Application for Counterparty Risk Analysis
Orchestrates the entire risk analysis process
"""

import sys
import os
import logging
from pathlib import Path
import yaml
import pandas as pd
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_processor import RepoDataProcessor
from counterparty_analyzer import CounterpartyRiskAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('counterparty_risk_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CounterpartyRiskAnalysisApp:
    """
    Main application class for counterparty risk analysis
    """
    
    def __init__(self, config_path: str = "../config/risk_thresholds.yaml"):
        """Initialize the application"""
        self.config_path = config_path
        self.config = self._load_config()
        self.data_processor = RepoDataProcessor(config_path)
        self.analyzer = CounterpartyRiskAnalyzer(self.config)
        
    def _load_config(self) -> dict:
        """Load configuration file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise
    
    def run_analysis(self, data_file: str, output_dir: str = "../reports") -> dict:
        """
        Run the complete counterparty risk analysis
        
        Args:
            data_file: Path to the input CSV file
            output_dir: Directory to save output files
            
        Returns:
            Dictionary containing analysis results
        """
        logger.info("Starting Counterparty Risk Analysis")
        logger.info(f"Input file: {data_file}")
        logger.info(f"Output directory: {output_dir}")
        
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        try:
            # Step 1: Load and process data
            logger.info("Step 1: Loading and processing data...")
            self.data_processor.load_data(data_file)
            self.data_processor.clean_data()
            processed_data = self.data_processor.derive_risk_fields()
            
            # Step 2: Analyze counterparties
            logger.info("Step 2: Analyzing counterparty risk...")
            analysis_results = self.analyzer.analyze_counterparties(processed_data)
            
            # Step 3: Generate reports
            logger.info("Step 3: Generating reports...")
            self._generate_reports(analysis_results, processed_data, output_dir)
            
            # Step 4: Print summary
            logger.info("Step 4: Analysis summary...")
            self._print_summary(analysis_results)
            
            logger.info("Counterparty Risk Analysis completed successfully!")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            raise
    
    def _generate_reports(self, analysis_results: dict, processed_data: pd.DataFrame, output_dir: str):
        """Generate various reports and outputs"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Save processed data
        processed_data_file = os.path.join(output_dir, f"processed_data_{timestamp}.csv")
        self.data_processor.save_processed_data(processed_data_file)
        
        # 2. Export analysis results to Excel
        excel_file = os.path.join(output_dir, f"counterparty_risk_analysis_{timestamp}.xlsx")
        self.analyzer.export_analysis_results(excel_file)
        
        # 3. Generate risk summary report
        summary_file = os.path.join(output_dir, f"risk_summary_{timestamp}.txt")
        self._generate_summary_report(analysis_results, summary_file)
        
        # 4. Generate high-risk counterparty report
        high_risk_file = os.path.join(output_dir, f"high_risk_counterparties_{timestamp}.csv")
        high_risk_df = self.analyzer.get_high_risk_counterparties_details()
        if not high_risk_df.empty:
            high_risk_df.to_csv(high_risk_file, index=False)
            logger.info(f"High-risk counterparties report saved to {high_risk_file}")
        
        # 5. Generate portfolio risk dashboard data
        dashboard_file = os.path.join(output_dir, f"dashboard_data_{timestamp}.json")
        self._generate_dashboard_data(analysis_results, dashboard_file)
        
        logger.info(f"All reports generated in {output_dir}")
    
    def _generate_summary_report(self, analysis_results: dict, file_path: str):
        """Generate a text summary report"""
        with open(file_path, 'w') as f:
            f.write("COUNTERPARTY RISK ANALYSIS SUMMARY REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            # Analysis metadata
            summary = analysis_results['summary']
            f.write(f"Analysis Date: {summary['analysis_date']}\n")
            f.write(f"Total Counterparties: {summary['total_counterparties']}\n")
            f.write(f"High-Risk Counterparties: {summary['high_risk_counterparties']}\n")
            f.write(f"Portfolio Risk Level: {summary['portfolio_risk_level']}\n\n")
            
            # Key risk metrics
            f.write("KEY RISK METRICS:\n")
            f.write("-" * 20 + "\n")
            key_metrics = summary['key_risk_metrics']
            f.write(f"Average Risk Score: {key_metrics['average_risk_score']:.3f}\n")
            f.write(f"High Risk Exposure: {key_metrics['high_risk_exposure_pct']:.2f}%\n")
            f.write(f"Critical Risk Exposure: {key_metrics['critical_risk_exposure_pct']:.2f}%\n")
            f.write(f"Concentration HHI: {key_metrics['concentration_hhi']:.3f}\n\n")
            
            # Portfolio aggregates
            portfolio = analysis_results['portfolio_aggregates']
            f.write("PORTFOLIO OVERVIEW:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Total Portfolio Exposure: ${portfolio['total_portfolio_exposure']:,.2f}M\n")
            f.write(f"Total Trades: {portfolio['total_trades']:,}\n")
            f.write(f"Unique Counterparties: {portfolio['unique_counterparties']}\n\n")
            
            # Risk distribution
            f.write("RISK DISTRIBUTION:\n")
            f.write("-" * 20 + "\n")
            for category, count in portfolio['risk_distribution'].items():
                f.write(f"{category}: {count} trades\n")
            f.write("\n")
            
            # High-risk counterparties
            if analysis_results['high_risk_counterparties']:
                f.write("HIGH-RISK COUNTERPARTIES:\n")
                f.write("-" * 25 + "\n")
                for counterparty in analysis_results['high_risk_counterparties']:
                    profile = analysis_results['counterparty_profiles'][counterparty]
                    f.write(f"- {counterparty}: ${profile.total_exposure:,.2f}M, "
                           f"Risk Score: {profile.average_risk_score:.3f}\n")
                f.write("\n")
            
            # Risk flags and recommendations
            risk_flags = analysis_results['risk_flags']
            if risk_flags['critical_flags']:
                f.write("CRITICAL RISK FLAGS:\n")
                f.write("-" * 20 + "\n")
                for flag in risk_flags['critical_flags']:
                    f.write(f"⚠️  {flag}\n")
                f.write("\n")
            
            if risk_flags['high_flags']:
                f.write("HIGH RISK FLAGS:\n")
                f.write("-" * 15 + "\n")
                for flag in risk_flags['high_flags']:
                    f.write(f"🔴 {flag}\n")
                f.write("\n")
            
            if risk_flags['recommendations']:
                f.write("RECOMMENDATIONS:\n")
                f.write("-" * 15 + "\n")
                for i, rec in enumerate(risk_flags['recommendations'], 1):
                    f.write(f"{i}. {rec}\n")
        
        logger.info(f"Summary report saved to {file_path}")
    
    def _generate_dashboard_data(self, analysis_results: dict, file_path: str):
        """Generate JSON data for dashboard visualization"""
        import json
        
        dashboard_data = {
            'summary': analysis_results['summary'],
            'portfolio_metrics': {
                'total_exposure': analysis_results['portfolio_aggregates']['total_portfolio_exposure'],
                'average_risk_score': analysis_results['portfolio_aggregates']['portfolio_average_risk_score'],
                'high_risk_exposure_pct': analysis_results['portfolio_aggregates']['high_risk_exposure_pct'],
                'critical_risk_exposure_pct': analysis_results['portfolio_aggregates']['critical_risk_exposure_pct']
            },
            'counterparty_data': [
                {
                    'counterparty_type': profile.counterparty_type,
                    'total_exposure': profile.total_exposure,
                    'average_risk_score': profile.average_risk_score,
                    'risk_category': profile.risk_category,
                    'concentration_ratio': profile.concentration_ratio,
                    'is_high_risk': profile.counterparty_type in analysis_results['high_risk_counterparties']
                }
                for profile in analysis_results['counterparty_profiles'].values()
            ],
            'risk_distribution': analysis_results['portfolio_aggregates']['risk_distribution'],
            'concentration_metrics': analysis_results['portfolio_aggregates']['counterparty_concentration']
        }
        
        with open(file_path, 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        
        logger.info(f"Dashboard data saved to {file_path}")
    
    def _print_summary(self, analysis_results: dict):
        """Print analysis summary to console"""
        summary = analysis_results['summary']
        portfolio = analysis_results['portfolio_aggregates']
        
        print("\n" + "="*60)
        print("COUNTERPARTY RISK ANALYSIS SUMMARY")
        print("="*60)
        print(f"Analysis Date: {summary['analysis_date']}")
        print(f"Portfolio Risk Level: {summary['portfolio_risk_level']}")
        print(f"Total Exposure: ${portfolio['total_portfolio_exposure']:,.2f}M")
        print(f"Total Trades: {portfolio['total_trades']:,}")
        print(f"Unique Counterparties: {portfolio['unique_counterparties']}")
        print(f"High-Risk Counterparties: {summary['high_risk_counterparties']}")
        print(f"Average Risk Score: {summary['key_risk_metrics']['average_risk_score']:.3f}")
        print(f"High Risk Exposure: {summary['key_risk_metrics']['high_risk_exposure_pct']:.2f}%")
        print(f"Critical Risk Exposure: {summary['key_risk_metrics']['critical_risk_exposure_pct']:.2f}%")
        
        if analysis_results['high_risk_counterparties']:
            print(f"\n🔴 HIGH-RISK COUNTERPARTIES:")
            for counterparty in analysis_results['high_risk_counterparties']:
                profile = analysis_results['counterparty_profiles'][counterparty]
                print(f"   • {counterparty}: ${profile.total_exposure:,.2f}M (Risk: {profile.average_risk_score:.3f})")
        
        risk_flags = analysis_results['risk_flags']
        if risk_flags['critical_flags']:
            print(f"\n⚠️  CRITICAL RISK FLAGS:")
            for flag in risk_flags['critical_flags']:
                print(f"   • {flag}")
        
        print("="*60)

def main():
    """Main function to run the analysis"""
    # Default file paths
    data_file = "data/repo_simulation_with_cash_legs.csv"
    config_file = "config/risk_thresholds.yaml"
    output_dir = "reports"
    
    # Check if data file exists
    if not os.path.exists(data_file):
        logger.error(f"Data file not found: {data_file}")
        print(f"Please ensure the data file exists at: {data_file}")
        return
    
    try:
        # Initialize and run analysis
        app = CounterpartyRiskAnalysisApp(config_file)
        results = app.run_analysis(data_file, output_dir)
        
        print(f"\n✅ Analysis completed successfully!")
        print(f"📊 Reports saved to: {output_dir}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print(f"❌ Analysis failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
