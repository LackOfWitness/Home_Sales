<div align="center">
  <img src="Module 22 Github Graphic.jpg" alt="Home Sales Analysis with PySpark SQL" style="border: 2px solid #0366d6; border-radius: 8px; background: #f6f8fa; padding: 20px; max-width: 800px; width: 100%;">
</div>

# Home Sales Analysis with PySpark SQL

## Overview
This project uses PySpark SQL to analyze home sales data, focusing on key metrics like average prices based on various home features.
The analysis includes data caching and partitioning techniques to optimize query performance.

## Technical Details

### Environment Setup
- PySpark SQL for data processing and analysis
- SparkSession configured with "SparkSQL" app name
- Jupyter Notebook for interactive analysis
- Spark Version: 3.5.3

### Key Functionality
1. Data Loading and Processing
   - Read home sales data from AWS S3 bucket
   - Created temporary view for SQL querying
   - Implemented data caching and partitioning

2. Analysis Features
   - Average price calculations for homes with specific criteria:
     - 4 bedroom homes by year
     - 3 bedroom, 3 bathroom homes by year built
     - 3 bedroom, 3 bathroom, 2 floor homes ≥2000 sqft
     - Average price by view rating for homes ≥$350,000

3. Performance Optimization
   - Cached temporary tables for faster query execution
   - Implemented partitioning by date_built
   - Compared query runtime between cached and uncached data

### Key Findings
- Successfully analyzed home price trends across different property specifications
- Demonstrated performance improvements through caching and partitioning
- Validated data processing through cached table verification

## Technical Implementation
The analysis was performed using:
- PySpark SQL for data querying
- Parquet file format for data storage
- Partition-based optimization
- Memory caching for performance enhancement

### Detailed Partitioning Strategy with Parquet
The project implements advanced data organization using Parquet partitioning:

1. **Parquet Format Benefits**
   - Columnar storage format optimized for big data processing
   - Efficient compression and encoding schemes
   - Reduced I/O operations during query execution
   - Better handling of complex nested data structures

2. **Partitioning Implementation**
   ```python
   # Partition by date_built field
   df.write.partitionBy("date_built").mode("overwrite").parquet("home_sales_partitioned_parquet")
   ```
   - Creates separate directories for each unique `date_built` value
   - Enables partition pruning during queries
   - Improves query performance for date-based filtering

3. **Performance Comparison**
   - Query execution times were measured across three scenarios:
     1. Regular DataFrame queries
     2. Cached table queries
     3. Partitioned Parquet queries
   - Partitioning showed particular efficiency for:
     - Year-specific analysis
     - Time-series based aggregations
     - Filtered queries on the `date_built` column

4. **Partition Structure**
   ```
   home_sales_partitioned_parquet/
   ├── date_built=2010/
   ├── date_built=2011/
   ├── date_built=2012/
   └── ...
   ```
   - Each partition contains only records for that specific year
   - Enables Spark to skip irrelevant partitions during query execution

## Repository Structure
- `Home_Sales.ipynb`: Main Jupyter notebook containing analysis code
- Data source: AWS S3 bucket containing home sales data

## Dependencies
- PySpark
- Python 3.x
- Jupyter Notebook

## Detailed Dependencies
1. **Core Libraries**
   - PySpark SQL
   - findspark
   - os (Python standard library)
   - time (Python standard library)

2. **PySpark Components**
   - SparkSession
   - SparkFiles
   - SparkContext (implicit)

3. **Environment Requirements**
   - Java JDK 11 (OpenJDK)
   - Apache Spark 3.5.3
   - Hadoop 3.x (included with Spark)

4. **Development Environment**
   - Python 3.x
   - Jupyter Notebook/Google Colab
   - pip (Python package installer)

5. **Data Storage**
   - Parquet file format support
   - AWS S3 access capabilities


## Execution Instructions
1. Ensure PySpark is properly installed
2. Open Home_Sales.ipynb in Jupyter Notebook
3. Execute cells sequentially to reproduce analysis

## Contact Information
For questions or feedback about this analysis, please contact:
- GitHub: [[Sergei N. Sergeev](https://github.com/LackOfWitness)]
- Email: [sergei.n.sergeev@gmail.com](mailto:sergei.n.sergeev@gmail.com)
- LinkedIn: [Sergei Sergeev](https://www.linkedin.com/in/sergei-sergeev-7b4a31290/)

## License
This project is licensed under the MIT License - see below for details:

MIT License

Copyright (c) 2024 [Your Name]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## References
Data provided by [edX Boot Camps LLC](https://www.edx.org/) for educational purposes.