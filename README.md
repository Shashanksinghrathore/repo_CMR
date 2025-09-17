
# Counterparty Risk Analysis System


A comprehensive system for analyzing counterparty market risk in repo trading portfolios. This system processes real-world repo trade data, derives additional risk fields, and identifies high-risk counterparties based on industry-standard risk thresholds.

## Features

### 🔍 **Data Processing & Risk Field Derivation**
- Loads and cleans repo trade data from CSV files
- Derives 20+ additional risk fields including:
  - Credit risk scores and rating weights
  - Market risk metrics (collateral quality, haircut risk, specialness)
  - Liquidity risk measures (encumbrance, margin call risk)
  - Operational risk indicators (wrong-way risk, cross-currency risk)
  - Term risk assessments
  - Stress testing scenarios

### 📊 **Counterparty Risk Analysis**
- Aggregates risk metrics at counterparty level
- Identifies high-risk counterparties using multiple criteria
- Calculates concentration risk metrics (HHI, top-5 concentration)
- Generates comprehensive risk profiles for each counterparty type

### 🚨 **Risk Monitoring & Alerting**
- Real-time risk flag generation
- Multi-level risk categorization (Low, Medium, High, Critical)
- Automated recommendations for risk mitigation
- Escalation thresholds for different risk levels

### 📈 **Reporting & Visualization**
- Comprehensive Excel reports with multiple sheets
- Text-based summary reports
- JSON data for dashboard integration
- High-risk counterparty specific reports

## System Architecture

```
counterparty_risk_analysis/
├── src/
│   ├── data_processor.py      # Data loading, cleaning, and risk field derivation
│   ├── counterparty_analyzer.py # Counterparty risk analysis and aggregation
│   └── main.py               # Main application orchestrator
├── config/
│   └── risk_thresholds.yaml  # Risk thresholds and configuration
├── data/
│   └── repo_simulation_with_cash_legs.csv # Input data file
├── reports/                  # Generated reports and outputs
├── notebooks/               # Jupyter notebooks for analysis
└── requirements.txt         # Python dependencies
```

## Risk Framework

### Credit Risk
- **Rating-based risk weights** (Basel III inspired)
- **High-risk rating flags** for counterparties below investment grade
- **Concentration risk** monitoring (single counterparty limits)

### Market Risk
- **Collateral quality scoring** (liquidity, volatility, duration)
- **Haircut risk assessment** based on collateral type
- **Specialness risk** for repo-specific market dynamics

### Liquidity Risk
- **Encumbrance risk** from long-term collateral commitments
- **Margin call risk** from potential funding shortfalls
- **HQLA level** monitoring for regulatory compliance

### Operational Risk
- **Wrong-way risk** identification
- **Cross-currency risk** assessment
- **CCP clearing** compliance monitoring

### Term Risk
- **Maturity risk** from long-term exposures
- **Open repo risk** from indefinite maturity trades

## Installation

1. **Clone or download the project**
```bash
cd counterparty_risk_analysis
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Verify data file location**
Ensure your `repo_simulation_dataset_with_cash_legs.csv` is in the `data/` directory.

## Usage

### Quick Start
```bash
cd src
python main.py
```

### Programmatic Usage
```python
from src.main import CounterpartyRiskAnalysisApp

# Initialize the application
app = CounterpartyRiskAnalysisApp("../config/risk_thresholds.yaml")

# Run analysis
results = app.run_analysis(
    data_file="../data/repo_simulation_with_cash_legs.csv",
    output_dir="../reports"
)
```

### Configuration
The system uses a YAML configuration file (`config/risk_thresholds.yaml`) that defines:
- Risk scoring weights
- Threshold values for different risk categories
- Alert and escalation levels
- Regulatory compliance limits

## Output Files

The system generates several output files in the `reports/` directory:

1. **`processed_data_YYYYMMDD_HHMMSS.csv`** - Enhanced dataset with all derived risk fields
2. **`counterparty_risk_analysis_YYYYMMDD_HHMMSS.xlsx`** - Comprehensive Excel report with multiple sheets:
   - Portfolio Summary
   - Counterparty Profiles
   - High-Risk Counterparties
3. **`risk_summary_YYYYMMDD_HHMMSS.txt`** - Text-based summary report
4. **`high_risk_counterparties_YYYYMMDD_HHMMSS.csv`** - Detailed high-risk counterparty data
5. **`dashboard_data_YYYYMMDD_HHMMSS.json`** - JSON data for dashboard integration

## Risk Thresholds

The system uses industry-standard risk thresholds based on:

### Regulatory Standards
- **Basel III** credit risk weights
- **LCR/NSFR** liquidity requirements
- **CCP clearing** mandates

### Industry Best Practices
- **Concentration limits** (25% single counterparty, 40% sector)
- **Haircut thresholds** (20% maximum acceptable)
- **Specialness limits** (50bp maximum)

### Risk Scoring
- **Composite risk score** (0-1 scale) combining all risk factors
- **Risk categories**: Low (≤0.3), Medium (≤0.6), High (≤0.8), Critical (>0.8)

## High-Risk Counterparty Identification

A counterparty is flagged as high-risk if it meets 3 or more of these criteria:
- Average risk score > 0.8
- Maximum risk score > 0.9
- Concentration ratio > 25%
- >30% of trades are high-risk
- Any critical risk trades
- Component risk scores > 0.7

## Example Output

```
============================================================
COUNTERPARTY RISK ANALYSIS SUMMARY
============================================================
Analysis Date: 2025-01-01 12:00:00
Portfolio Risk Level: MEDIUM
Total Exposure: $1,234.56M
Total Trades: 2,500
Unique Counterparties: 8
High-Risk Counterparties: 2
Average Risk Score: 0.456
High Risk Exposure: 18.75%
Critical Risk Exposure: 3.25%

🔴 HIGH-RISK COUNTERPARTIES:
   • HedgeFund: $156.78M (Risk: 0.823)
   • Dealer: $89.45M (Risk: 0.756)

⚠️  CRITICAL RISK FLAGS:
   • HIGH_PORTFOLIO_CONCENTRATION
============================================================
```

## Customization

### Adding New Risk Fields
1. Modify `data_processor.py` to add new risk field derivation methods
2. Update the `derive_risk_fields()` method to include new calculations
3. Add corresponding thresholds to `config/risk_thresholds.yaml`

### Adjusting Risk Thresholds
Edit `config/risk_thresholds.yaml` to modify:
- Risk scoring weights
- Threshold values
- Alert levels
- Concentration limits

### Extending Analysis
- Add new counterparty types in the analyzer
- Implement additional risk metrics
- Create custom reporting templates

## Dependencies

- **pandas** - Data manipulation and analysis
- **numpy** - Numerical computations
- **scipy** - Statistical functions
- **scikit-learn** - Machine learning utilities
- **matplotlib/seaborn** - Data visualization
- **plotly/dash** - Interactive dashboards
- **pyyaml** - Configuration file parsing
- **openpyxl/xlsxwriter** - Excel file generation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or support, please contact the development team or create an issue in the repository.

---

**Note**: This system is designed for educational and research purposes. For production use in financial institutions, additional validation, testing, and regulatory compliance measures should be implemented.
