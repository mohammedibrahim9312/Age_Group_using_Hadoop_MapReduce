##Age Group Partitioning using Hadoop MapReduce

## Project Overview
This project analyzes large-scale demographic data using Hadoop MapReduce.

The system:
- Partitions records by age group using a Custom Partitioner
- Calculates average income per age group
- Uses Combiner optimization to reduce shuffle traffic
- Performs robust data validation and error handling

Each reducer processes exactly one age group.

---

## Technologies Used
- Hadoop MapReduce
- Java
- HDFS
- Custom Partitioner
- Combiner Optimization
- Cloudera QuickStart VM

---

## Dataset
The original dataset was obtained from IPUMS USA census microdata.

Dataset fields:
- person_id
- age
- income
- employment_status
- education_level

Original dataset size:
- 60,246,841 records

Included in this repository:
- `sample_data.csv` (cleaned sample with 2000 rows)

> Due to dataset licensing and file size limitations, only a cleaned sample of the dataset is included in this repository.

---

## Project Structure

```text
agegroup/
│
├── AgeGroupMapper.java
├── AgeGroupPartitioner.java
├── AgeGroupCombiner.java
├── AgeGroupReducer.java
├── AgeGroupDriverNoCombiner.java
├── AgeGroupDriverWithCombiner.java
│
├── sample_data.csv
├── output_no_combiner.txt
├── output_with_combiner.txt
│
└── README.md
```

---

## Files Description

| File | Description |
|---|---|
| AgeGroupMapper.java | Reads and validates input records |
| AgeGroupPartitioner.java | Routes age groups to specific reducers |
| AgeGroupCombiner.java | Performs local aggregation before shuffle |
| AgeGroupReducer.java | Computes average income |
| AgeGroupDriverNoCombiner.java | Runs job without Combiner |
| AgeGroupDriverWithCombiner.java | Runs job with Combiner |

---

## Input Format

```text
person_id, age, income, employment_status, education_level
```

Example:

```text
P001,25,3000,Employed,Bachelor
```

---

## Age Groups

| Age Range | Reducer |
|---|---|
| 18-24 | Reducer 0 |
| 25-34 | Reducer 1 |
| 35-44 | Reducer 2 |
| 45-54 | Reducer 3 |
| 55+ | Reducer 4 |

---

## Data Validation

The system validates:
- Malformed lines
- Invalid age values
- Negative income values
- Unparseable numeric values
- Missing fields

Invalid records are skipped and tracked using Hadoop Counters.

---

## How to Compile

```bash
javac -classpath $(hadoop classpath) -d classes *.java
jar -cvf agegroup.jar -C classes .
```

---

## How to Run

### Run WITHOUT Combiner

```bash
hadoop jar agegroup.jar agegroup.AgeGroupDriverNoCombiner \
/task15/sample_data.csv /output_no_combiner
```

### Run WITH Combiner

```bash
hadoop jar agegroup.jar agegroup.AgeGroupDriverWithCombiner \
/task15/sample_data.csv /output_with_combiner
```

---

## Sample Output

```text
18-24   Avg Income: 13529
25-34   Avg Income: 37877
35-44   Avg Income: 53656
45-54   Avg Income: 57410
55+     Avg Income: 51208
```

---

## Combiner Performance Analysis

### WITHOUT Combiner
- Reduce input records: 60,246,840
- Reduce shuffle bytes: ~1 GB
- Spilled Records: 180M+
- Execution time: significantly higher

### WITH Combiner
- Reduce input records: 185
- Reduce shuffle bytes: ~6 KB
- Spilled Records: 550
- Execution time: significantly lower

### Performance Impact
- Shuffle traffic reduced by 99.9%
- Spilled records reduced by 99.9%
- Faster MapReduce execution
- Lower network overhead

---

## Key Features
- Custom Hadoop Partitioner
- Combiner optimization
- Distributed processing
- Big Data handling
- Data validation
- Error handling
- Hadoop Counters monitoring

---

## Future Improvements
- Apache Spark implementation
- Real-time stream processing
- Machine learning analytics
- Advanced demographic insights


