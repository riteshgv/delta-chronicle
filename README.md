# delta-chronicle

> Cross-table temporal causality engine for Delta Lake

[![CI](https://github.com/riteshgv/delta-chronicle/actions/workflows/ci.yml/badge.svg)](https://github.com/riteshgv/delta-chronicle/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](Dockerfile)

**Delta Lake shows you *that* your data changed.  
delta-chronicle shows you *which upstream table caused it*.**

```
gold.driver_revenue  ← revenue wrong since Jan 15
       ↑
silver.trip_enriched ← reprocessed with bad data  
       ↑
bronze.taxi_trips    ← 🔴 ROOT CAUSE: vendor_id=1 fares ×10