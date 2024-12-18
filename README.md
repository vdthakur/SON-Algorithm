# **SON Algorithm with PySpark**

## **Project Overview**
This project implements the SON (Sequential Algorithm) to identify frequent itemsets in large datasets. The SON algorithm is applied in two tasks using the Spark framework. Part 1 uses a small simulated dataset, while Part 2 uses the real-world Ta Feng dataset. The project showcases distributed computing techniques to handle massive datasets effectively. 

The **Apriori algorithm** is used as the limited-pass algorithm in this implementation to efficiently process chunks of the dataset.

---

## **Project Structure**

### **Part 1: Simulated Dataset**
- **Objective**: Generate market-basket models for both businesses and users using a small dataset.
- **Cases**:
  - **Case 1**: Frequent businesses in user baskets.
  - **Case 2**: Frequent users in business baskets.
- **Input**:
  - Case Number (1 or 2)
  - Support threshold
  - Input file path
  - Output file path
- **Output**:
  - Runtime of execution
  - **Intermediate results**: Candidate itemsets after the first SON pass.
  - **Final results**: Frequent itemsets after SON execution.
  - Both intermediate and final results are saved in a single file.

---

### **Part 2: Ta Feng Dataset**
- **Objective**: Process the Ta Feng dataset to generate frequent itemsets and identify customers with frequent purchases.
- **Steps**:
  1. **Preprocessing**:
     - Extract `CUSTOMER_ID`, `PRODUCT_ID`, and `TRANSACTION_DT`.
     - Create daily transactions per customer (`DATE-CUSTOMER_ID`).
     - Save the data as a CSV file.
  2. **SON Algorithm**:
     - Filter transactions with a threshold for the number of items (`k`).
     - Apply the SON algorithm to generate candidates and frequent itemsets.

---

## **Apriori Algorithm**
The **Apriori algorithm** was used as the limited-pass algorithm in the SON implementation. This algorithm is effective for identifying frequent itemsets by:
- Generating candidates based on subsets of known frequent itemsets.
- Reducing computational complexity by leveraging the anti-monotonic property (if an itemset is frequent, all its subsets are also frequent).

---

## **Input Format**

### Task 1
- **Case Number**: `1` or `2`.
- **Support**: Minimum count to qualify as a frequent itemset.
- **Input file path**: Path to the CSV file with the dataset.
- **Output file path**: Path where results will be saved.

### Task 2
- **Filter Threshold**: Minimum number of items in a transaction to qualify.
- **Support**: Minimum count to qualify as a frequent itemset.
- **Input file path**: Path to the preprocessed `customer_product` CSV file.
- **Output file path**: Path where results will be saved.

---

## **Output Format**

For both tasks:
1. **Runtime**:
   - Total execution time logged with the tag `Duration`, e.g., `Duration: 100`.
2. **Output File**:
   - **Intermediate Results**:
     - Labeled as `Candidates:` followed by candidate itemsets after the first pass.
   - **Final Results**:
     - Labeled as `Frequent Itemsets:` followed by the frequent itemsets.

---

### 2. **Task 2: Ta Feng Dataset**
- **Objective**: Process the Ta Feng dataset to generate frequent itemsets and identify customers with frequent purchases.

#### **Steps**
1. **Preprocessing**:
   - Extract `CUSTOMER_ID`, `PRODUCT_ID`, and `TRANSACTION_DT`.
   - Create daily transactions per customer (`DATE-CUSTOMER_ID`).
   - Save the data as a CSV file.
   
2. **SON Algorithm**:
   - Filter transactions with a threshold for the number of items (`k`).
   - Apply the SON algorithm to generate candidates and frequent itemsets using the **Apriori algorithm**.

#### **Input**
- **Filter Threshold**: Minimum number of items in a transaction to qualify.
- **Support**: Minimum count to qualify as a frequent itemset.
- **Input file path**: Path to the preprocessed `customer_product` CSV file.
- **Output file path**: Path where results will be saved.

#### **Output**
- **Runtime of execution**: Logged with the tag `Duration`, e.g., `Duration: 100`.
- **Intermediate and Final Results**:
  - Intermediate results: Candidate itemsets after the first pass, labeled as `Candidates:`.
  - Final results: Frequent itemsets, labeled as `Frequent Itemsets:`.
  - Both intermediate and final results are saved in a single file.

---

## **Execution Examples**

### Task 1
```bash
spark-submit part1.py <case_number> <support> <input_file_path> <output_file_path>

spark-submit part2.py <filter_threshold> <support> <input_file_path> <output_file_path>
```
===
