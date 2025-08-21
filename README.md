# CostUtilization

A comprehensive tool for performing cost and utilization characterization analysis across cloud infrastructure and resources.

## Overview

CostUtilization is a powerful analytics tool designed to help organizations understand, track, and optimize their infrastructure costs and resource utilization. It provides detailed insights into spending patterns, resource efficiency, and cost optimization opportunities.

## Features

- **Cost Analysis**: Detailed breakdown of costs by service, resource, and time period
- **Utilization Metrics**: Real-time and historical resource utilization tracking
- **Cost Forecasting**: Predictive analytics for future spending based on historical patterns
- **Multi-Cloud Support**: Compatible with AWS, Azure, GCP, and other cloud providers
- **Custom Reports**: Generate tailored reports for different stakeholders
- **Alert System**: Configurable alerts for cost thresholds and utilization anomalies
- **API Integration**: RESTful API for seamless integration with existing tools
- **Export Capabilities**: Export data in CSV, JSON, and PDF formats

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Virtual environment (recommended)

### Install from PyPI

```bash
pip install costutilization
```

### Install from Source

```bash
git clone https://github.com/yourusername/costutilization.git
cd costutilization
pip install -e .
```

### Docker Installation

```bash
docker pull costutilization/costutilization:latest
docker run -d -p 8080:8080 costutilization/costutilization
```

## Quick Start Guide

### Basic Usage

```python
from costutilization import CostAnalyzer

# Initialize the analyzer
analyzer = CostAnalyzer(
    cloud_provider="aws",
    credentials_path="~/.aws/credentials"
)

# Get current month costs
current_costs = analyzer.get_current_month_costs()
print(f"Current month spend: ${current_costs['total']}")

# Analyze resource utilization
utilization = analyzer.analyze_utilization(
    resource_type="ec2",
    time_range="last_7_days"
)
```

### CLI Usage

```bash
# Analyze costs for specific service
costutil analyze --service ec2 --period monthly

# Generate utilization report
costutil report --type utilization --format pdf --output report.pdf

# Set up cost alerts
costutil alert create --threshold 1000 --service all --notify email@example.com
```

## Main Functions Documentation

### CostAnalyzer Class

#### `__init__(cloud_provider, credentials_path=None, region=None)`
Initializes the CostAnalyzer with specified cloud provider settings.

**Parameters:**
- `cloud_provider` (str): Cloud provider name ('aws', 'azure', 'gcp')
- `credentials_path` (str, optional): Path to credentials file
- `region` (str, optional): Default region for analysis

#### `get_current_month_costs()`
Returns the current month's costs broken down by service.

**Returns:**
- dict: Cost breakdown with 'total' and service-specific costs

#### `analyze_utilization(resource_type, time_range)`
Analyzes resource utilization for specified resource type and time range.

**Parameters:**
- `resource_type` (str): Type of resource ('ec2', 'rds', 'lambda', etc.)
- `time_range` (str): Time range for analysis ('last_7_days', 'last_month', etc.)

**Returns:**
- dict: Utilization metrics including average, peak, and recommendations

#### `generate_forecast(months=3)`
Generates cost forecast for specified number of months.

**Parameters:**
- `months` (int): Number of months to forecast (default: 3)

**Returns:**
- dict: Forecasted costs with confidence intervals

#### `create_report(report_type, format='pdf')`
Creates comprehensive report based on analysis.

**Parameters:**
- `report_type` (str): Type of report ('cost', 'utilization', 'optimization')
- `format` (str): Output format ('pdf', 'csv', 'json')

**Returns:**
- str: Path to generated report file

## Examples

### Example 1: Cost Trend Analysis

```python
from costutilization import CostAnalyzer
import matplotlib.pyplot as plt

analyzer = CostAnalyzer(cloud_provider="aws")

# Get historical cost data
historical_costs = analyzer.get_historical_costs(months=6)

# Plot cost trends
plt.figure(figsize=(10, 6))
plt.plot(historical_costs['dates'], historical_costs['costs'])
plt.title('Cost Trend - Last 6 Months')
plt.xlabel('Date')
plt.ylabel('Cost ($)')
plt.show()
```

### Example 2: Resource Optimization

```python
from costutilization import CostAnalyzer, Optimizer

analyzer = CostAnalyzer(cloud_provider="azure")
optimizer = Optimizer(analyzer)

# Find underutilized resources
underutilized = optimizer.find_underutilized_resources(
    utilization_threshold=30,  # 30% utilization
    min_age_days=7
)

# Get optimization recommendations
recommendations = optimizer.get_recommendations()
for rec in recommendations:
    print(f"Resource: {rec['resource_id']}")
    print(f"Current Cost: ${rec['current_cost']}")
    print(f"Potential Savings: ${rec['potential_savings']}")
    print(f"Recommendation: {rec['action']}\n")
```

### Example 3: Multi-Cloud Cost Comparison

```python
from costutilization import MultiCloudAnalyzer

multi_analyzer = MultiCloudAnalyzer({
    'aws': {'credentials_path': '~/.aws/credentials'},
    'azure': {'credentials_path': '~/.azure/credentials'},
    'gcp': {'credentials_path': '~/.gcp/credentials'}
})

# Compare costs across clouds
comparison = multi_analyzer.compare_costs(service_type='compute')
print("Cost Comparison - Compute Services")
for provider, cost in comparison.items():
    print(f"{provider}: ${cost['monthly_cost']}")
```

## Requirements

### Python Dependencies

```
pandas>=1.3.0
numpy>=1.21.0
boto3>=1.18.0
azure-mgmt-costmanagement>=3.0.0
google-cloud-billing>=1.4.0
matplotlib>=3.4.0
seaborn>=0.11.0
requests>=2.26.0
click>=8.0.0
pyyaml>=5.4.0
jinja2>=3.0.0
```

### System Requirements

- **Memory**: Minimum 4GB RAM (8GB recommended for large datasets)
- **Storage**: 1GB free disk space for caching and reports
- **Network**: Internet connection for cloud API access
- **OS**: Linux, macOS, or Windows 10+

### Cloud Provider Requirements

- **AWS**: IAM role or user with Cost Explorer and CloudWatch access
- **Azure**: Service Principal with Cost Management Reader role
- **GCP**: Service Account with Billing Viewer and Monitoring Viewer roles

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues, questions, or contributions, please visit our [GitHub repository](https://github.com/yourusername/costutilization) or contact us at support@costutilization.io.