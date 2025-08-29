# Naming Conventions

This document outlines the naming conventions used for ...

## Table of Contents

1. [**General Principles**](#general-principles)
2. [**Naming Conventions**](#naming-conventions)
    - [**Bronze Rules**](bronze-rules)
    - [**Silver Rules**](silver-rules)
    - [**Gold Rules**](gold-rules)


## General Principles
- **Naming Conventions**: Use snake_case, with lowercase letters and underscores (`_`) to separate words.
- **Language**: Use English for all names.

## Naming Conventions
### Bronze Rules
- All names must start with the source system name, and date must match the date of data.
- `<sourcesystem>_<entity>_<date>.parquet`
    - `<sourcesystem>`: Name of the source system (e.g., `ohlc`, `company`, `marketstatus`, `newssemtiment`)
    - `<date>`: Date of data from source system. Format in `YY-MM-DD`
    - Example: `polygon_ohlc_2025-01-01.parquet` -> OHLC data from the Polygon system at 2025-01-01.

### Silver Rules
- All names 

### Gold Rules 
