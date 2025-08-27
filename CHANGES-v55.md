# CostUtilization CDM v5.5 Migration Summary

This document summarizes the comprehensive updates made to align the CostUtilization package with OMOP CDM v5.5 and implement modern R practices.

## 🎯 Objectives Completed

### 1. ✅ CDM v5.5 Compatibility
- **Full support for long-format COST table** with enhanced temporal fields
- **Backward compatibility** maintained through transformation functions
- **Standardized cost concepts** using OMOP vocabulary

### 2. ✅ Modern R Implementation  
- **Settings-based API** using `createCostOfCareSettings()`
- **Tidyverse integration** with `dplyr`, `purrr`, and `rlang`
- **Functional programming** patterns for clean, maintainable code

### 3. ✅ Comprehensive Documentation
- **New vignettes** specifically for CDM v5.5
- **Updated examples** using modern R practices
- **Migration guides** for existing users

### 4. ✅ Enhanced Testing
- **CDM v5.5 compatibility tests** 
- **Data quality validation** tests
- **Multi-concept analysis** tests

---

## 📁 New Files Created

### Vignettes
- `vignettes/getting-started-v55.Rmd` - Updated getting started guide for CDM v5.5
- `vignettes/eunomia-setup-v55.Rmd` - Comprehensive Eunomia setup with CDM v5.5
- `vignettes/advanced-cost-analysis-v55.Rmd` - Advanced analysis techniques

### Tests
- `tests/testthat/test-cdm-v55-compatibility.R` - Comprehensive CDM v5.5 tests

### Documentation
- `R/CostUtilization-package-v55.R` - Updated package documentation
- `README-v55.md` - Updated README for CDM v5.5
- `vignettes/_pkgdown-v55.yml` - Updated vignette index

### Summary
- `CHANGES-v55.md` - This summary document

---

## 🔧 Key Technical Changes

### 1. Enhanced `transformCostToCdmV5dot5()` Function

**Location**: `R/EunomiaTestHelpers.R`

**Key Features**:
- Transforms wide-format cost tables to CDM v5.5 long format
- Creates proper cost concept mappings (31973, 31985, 31980, 31981, etc.)
- Adds new temporal fields (`effective_date`, `billed_date`, `paid_date`)
- Maintains referential integrity with clinical events
- Creates optimized indexes for performance

**Example Usage**:
```r
transformCostToCdmV5dot5(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  createIndexes = TRUE
)
```

### 2. Settings-Based API with `createCostOfCareSettings()`

**Location**: `R/createCostOfCareSettings.R`

**Key Improvements**:
- Validates all input parameters
- Supports complex event filtering
- Enables micro-costing analysis
- Provides clear error messages
- Maintains backward compatibility

**Example Usage**:
```r
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  eventFilters = diabetesFilters,
  costConceptId = 31973L
)
```

### 3. Enhanced `calculateCostOfCare()` Function

**Location**: `R/CalculateCostOfCare.R`

**Key Features**:
- Uses settings objects for cleaner API
- Returns comprehensive diagnostics
- Supports multiple cost concepts
- Provides detailed patient flow tracking
- Optimized for CDM v5.5 long format

---

## 📊 CDM v5.5 Cost Table Structure

### Long Format Benefits

**Before (Wide Format)**:
```sql
CREATE TABLE cost (
  cost_id BIGINT,
  person_id BIGINT,
  total_charge FLOAT,
  paid_by_payer FLOAT,
  paid_by_patient FLOAT,
  -- ... many cost columns
);
```

**After (CDM v5.5 Long Format)**:
```sql
CREATE TABLE cost (
  cost_id BIGINT PRIMARY KEY,
  person_id BIGINT NOT NULL,
  cost_event_id BIGINT,
  visit_occurrence_id BIGINT,
  visit_detail_id BIGINT,
  cost_domain_id VARCHAR(50),
  effective_date DATE NOT NULL,
  cost_event_field_concept_id INT,
  cost_concept_id INT NOT NULL,
  cost_type_concept_id INT,
  cost_source_concept_id INT,
  cost_source_value VARCHAR(50),
  currency_concept_id INT,
  cost FLOAT,
  incurred_date DATE,
  billed_date DATE,
  paid_date DATE,
  -- ... other fields
);
```

### Standard Cost Concepts

| Concept ID | Description | Source Value |
|------------|-------------|--------------|
| 31973 | Total charge | "total_charge" |
| 31985 | Total cost | "total_cost" |
| 31980 | Paid by payer | "paid_by_payer" |
| 31981 | Paid by patient | "paid_by_patient" |
| 31974 | Patient copay | "paid_patient_copay" |
| 31975 | Patient coinsurance | "paid_patient_coinsurance" |
| 31976 | Patient deductible | "paid_patient_deductible" |
| 31979 | Amount allowed | "amount_allowed" |

---

## 🧪 Testing Strategy

### Comprehensive Test Coverage

**CDM v5.5 Compatibility Tests**:
- Table structure validation
- Data type and constraint verification  
- Cost relationship preservation
- Temporal field consistency
- Index creation verification

**API Functionality Tests**:
- Settings object creation and validation
- Multi-concept analysis support
- Event filtering capabilities
- Diagnostic information accuracy

**Data Quality Tests**:
- Cost value reasonableness
- Referential integrity
- Temporal consistency
- Concept usage validation

---

## 📚 Documentation Updates

### New Vignette Structure

1. **Getting Started (CDM v5.5)** - Basic usage with new API
2. **Eunomia Setup (CDM v5.5)** - Comprehensive test environment setup
3. **Advanced Analysis (CDM v5.5)** - Complex analytical patterns
4. **CPI Adjustment** - Inflation adjustment techniques (updated)

### Modern R Examples

All examples now demonstrate:
- **Tidyverse patterns** using `dplyr`, `purrr`, `rlang`
- **Functional programming** with `map_dfr`, `pmap_dfr`
- **Tidy evaluation** with `!!sym()` for dynamic column references
- **Pipeline operations** with `|>` operator
- **Type-safe programming** with explicit `L` suffixes for integers

---

## 🔄 Migration Guide

### For Existing Users

**Step 1: Update Function Calls**
```r
# Old approach
results <- calculateCostOfCare(
  connection = connection,
  anchorCol = "cohort_start_date",
  startOffsetDays = 0,
  endOffsetDays = 365
)

# New approach  
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L
)

results <- calculateCostOfCare(
  connection = connection,
  costOfCareSettings = settings
)
```

**Step 2: Transform Cost Data**
```r
# Transform existing wide-format data
transformCostToCdmV5dot5(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "your_schema"
)
```

**Step 3: Update Analysis Code**
```r
# Use explicit cost concepts
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L  # Be explicit about cost type
)
```

---

## 🚀 Performance Improvements

### Database Optimizations

**Indexes Created**:
- `idx_cost_person_id` - Person-based queries
- `idx_cost_effective_date` - Temporal filtering
- `idx_cost_cost_concept_id` - Cost type filtering
- `idx_cost_visit_occurrence_id` - Visit-based analysis
- `idx_cost_person_date` - Combined person/date queries

**Query Optimizations**:
- Efficient SQL generation for long format
- Optimized joins with clinical event tables
- Batch processing support for large analyses

### R Code Optimizations

**Functional Programming**:
- Reduced code duplication with `purrr` functions
- Cleaner iteration patterns
- Better error handling

**Memory Efficiency**:
- Streaming results processing
- Efficient data transformations
- Reduced intermediate object creation

---

## 🎯 Benefits Achieved

### For Analysts
- **Cleaner API** with settings objects
- **Better error messages** and validation
- **Comprehensive diagnostics** for quality assurance
- **Modern R patterns** for maintainable code

### For Database Administrators  
- **Standardized cost structure** across databases
- **Improved query performance** with proper indexes
- **Better data lineage** with enhanced traceability

### For Researchers
- **More precise cost analysis** with granular concepts
- **Enhanced temporal analysis** with multiple date fields
- **Flexible event filtering** for complex research questions
- **Reproducible workflows** with validated settings

---

## 🔮 Future Enhancements

### Planned Features
- **CPI adjustment integration** with CDM v5.5 temporal fields
- **Multi-currency support** using currency concepts
- **Advanced visualization** functions for cost trajectories
- **Machine learning integration** for cost prediction

### Community Contributions
- **Concept set libraries** for common conditions
- **Validation rules** for cost data quality
- **Performance benchmarks** across different databases
- **Real-world examples** from OHDSI network sites

---

## 📞 Support and Resources

### Documentation
- **Package website**: https://ohdsi.github.io/CostUtilization/
- **Vignettes**: Comprehensive guides for all use cases
- **Function reference**: Detailed API documentation

### Community
- **OHDSI Forums**: https://forums.ohdsi.org/
- **GitHub Issues**: Bug reports and feature requests
- **MS Teams**: Real-time collaboration

### Training
- **OHDSI Tutorials**: Step-by-step learning materials
- **Webinars**: Regular training sessions
- **Workshops**: Hands-on learning opportunities

---

*This migration represents a significant advancement in healthcare cost analysis capabilities while maintaining the reliability and performance that users expect from OHDSI tools.*