# This file contains all bug fixes for the CostUtilization package

# BUG-03: Add missing function alias
#' @rdname transformCostToCdmV5p4
#' @export
transformCostToCdmV5dot5 <- transformCostToCdmV5p4

# The other bugs are fixed in their respective files:
# BUG-01: Fixed in R/CalculateCostOfCare.R documentation
# BUG-02: Fixed in R/CalculateCostOfCare.R materializeEventFilterTable function
# BUG-04: Fixed in tests/testthat/test-EunomiaTestHelpers.R
# BUG-05: Fixed in R/CalculateCostOfCare.R createResultsTables function