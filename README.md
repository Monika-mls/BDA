# BDA

# UNIT I – HDFS Storage

## Experiment 1: Hadoop Distributed File System Operations

REFERENCE: BDA RECORD PAGES 1–2

### 1. Aim

To create directories in HDFS, transfer files between the local file system and HDFS, and view files and directory contents using HDFS commands.

### 2. System Requirements

Hardware: Computer with minimum 4 GB RAM and sufficient disk space.

Software: Linux/Cloudera, Java, Hadoop, HDFS and terminal.

### 3. Theory

1. HDFS stands for Hadoop Distributed File System. It stores large files across multiple computers.
2. HDFS divides files into blocks and stores them on DataNodes.
3. The NameNode maintains file metadata and block locations.
4. `hadoop fs -mkdir` creates directories in HDFS.
5. `hadoop fs -put` and `-get` transfer files between local storage and HDFS.
6. `hadoop fs -ls` lists files, while `-cat` displays their contents.

### 4. Procedure

1. Start Hadoop and open the Linux terminal.
2. Create the required directory structure in HDFS.
3. Create a local text file containing sample data.
4. Upload the file into HDFS.
5. View the directory structure and file contents.
6. Download the file from HDFS to the local system.
7. Verify the downloaded file.

### 5. Program Code

```

# 1. Create HDFS directories
hadoop fs -mkdir -p /user/cloudera/mydata/raw
hadoop fs -mkdir -p /user/cloudera/mydata/processed

# 2. Create local file
echo "1,kriti,cse,45" > students.txt
echo "2,neha,ece,56" >> students.txt
echo "3,jyothi,ce,78" >> students.txt
echo "4,priya,mechanical,89" >> students.txt

# 3. Upload local file to HDFS
hadoop fs -put students.txt /user/cloudera/mydata/raw/

# 4. List directories and files
hadoop fs -ls /user/cloudera/mydata
hadoop fs -ls -R /user/cloudera/mydata

# 5. View HDFS file contents
hadoop fs -cat /user/cloudera/mydata/raw/students.txt

# 6. Download from HDFS to local
hadoop fs -get /user/cloudera/mydata/raw/students.txt downloaded.txt

# 7. Verify locally
cat downloaded.txt

```

### 6. Expected Output

```
1,kriti,cse,45
2,neha,ece,56
3,jyothi,ce,78
4,priya,mechanical,89
```

HDFS directory structure:

```
/user/cloudera/mydata/
                  ├── raw/
                  │    └── students.txt
                  └── processed/
```

### Important commands to memorize

| Command     | Purpose                      |
| ----------- | ---------------------------- |
| `-mkdir -p` | Create directories           |
| `-put`      | Local → HDFS                 |
| `-get`      | HDFS → Local                 |
| `-ls`       | List directory               |
| `-ls -R`    | List directories recursively |
| `-cat`      | View file contents           |

Exam tip: The same program covers Unit I sections A, B and C. You only need to change the emphasis in the Aim depending on the question.

# UNIT II – MapReduce Programming

## Experiment 2A: Word Count using Hadoop MapReduce

REFERENCE: BDA RECORD PAGES 3–4

### 1. Aim

To develop a Hadoop MapReduce program to calculate the frequency of each word in a text file.

### 2. System Requirements

Linux, Hadoop, Java JDK, HDFS and terminal.

### 3. Theory

1. MapReduce is a Hadoop framework used for distributed processing of large datasets.
2. The Mapper reads input text and emits `(word, 1)` for each word.
3. The Shuffle and Sort phase groups identical words.
4. The Reducer adds the values of each word.
5. The result contains each distinct word and its total frequency.
6. MapReduce enables parallel processing of large files across multiple nodes.

### 4. Procedure

1. Create a text file containing sample words.
2. Upload the file to HDFS.
3. Create the Java file `WordCount.java`.
4. Write Mapper, Reducer and Driver code.
5. Compile the Java file and create a JAR.
6. Execute the MapReduce program.
7. Display the output using HDFS commands.

### 5. Program Code

Save as `WordCount.java`.

```

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class Map extends Mapper<Object,Text,Text,IntWritable> {
        public void map(Object k, Text v, Context c)
                throws IOException, InterruptedException {
            for(String w : v.toString().split("\\s+"))
                if(!w.isEmpty())
                    c.write(new Text(w), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text k, Iterable<IntWritable> v, Context c)
                throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable n : v) sum += n.get();
            c.write(k, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Job j=Job.getInstance(new Configuration(),"WordCount");
        j.setJarByClass(WordCount.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j,new Path(args[0]));
        FileOutputFormat.setOutputPath(j,new Path(args[1]));
        System.exit(j.waitForCompletion(true)?0:1);
    }
}

```

#### Execution Commands

Create input:

```
echo "hadoop hadoop hdfs mapreduce" > input.txt
echo "hadoop hdfs mapreduce bigdata" >> input.txt
```

Upload to HDFS:

```
hadoop fs -mkdir -p /user/cloudera/input
hadoop fs -put input.txt /user/cloudera/input/
```

Create and edit Java file:

```
nano WordCount.java
```

Paste the Java program, then save using `Ctrl + O`, `Enter`, `Ctrl + X`.

Compile and create JAR:

```
javac -cp "$(hadoop classpath)" -d . WordCount.java
jar cf WordCount.jar WordCount*.class
```

Execute:

```
hadoop jar WordCount.jar WordCount /user/cloudera/input/input.txt /user/cloudera/output
```

View output:

```
hadoop fs -cat /user/cloudera/output/part-r-00000
```

### 6. Expected Output

```
bigdata     1
hadoop      3
hdfs        2
mapreduce   2
```

### Easy memory trick

MAP

(word, 1)

SHUFFLE

(word, [1,1])

REDUCE

(word, 2)

## Experiment 2B: Most Frequent Words Count

REFERENCE: BDA RECORD PAGE 4

### 1. Aim

To find and display the frequencies of words using the output generated by the Word Count program.

### 2. System Requirements

Linux, Hadoop, Java JDK, HDFS and terminal.

### 3. Theory

1. Most Frequent Words Count uses the output of the Word Count program.
2. The input contains words and their frequencies.
3. The Mapper swaps the key and value to produce `(frequency, word)`.
4. Hadoop sorts the frequencies in ascending order.
5. The Reducer writes the frequency and corresponding word.
6. The final result displays words ordered according to their frequencies.

### 4. Procedure

1. Execute the Word Count program and generate its output.
2. Create `MostFrequentWords.java`.
3. Read the Word Count output as input.
4. Swap the word and frequency in the Mapper.
5. Use one Reducer for ordered output.
6. Compile, create a JAR and execute the program.
7. View the output in HDFS.

### 5. Program Code

Save as `MostFrequentWords.java`.

```

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostFrequentWords {
    public static class Map extends Mapper<Object,Text,IntWritable,Text> {
        public void map(Object k, Text v, Context c)
                throws IOException, InterruptedException {
            String[] a=v.toString().trim().split("\\s+");
            if(a.length==2)
                c.write(new IntWritable(Integer.parseInt(a[1])),new Text(a[0]));
        }
    }

    public static class Reduce extends Reducer<IntWritable,Text,IntWritable,Text> {
        public void reduce(IntWritable k, Iterable<Text> v, Context c)
                throws IOException, InterruptedException {
            for(Text word:v) c.write(k,word);
        }
    }

    public static void main(String[] args) throws Exception {
        Job j=Job.getInstance(new Configuration(),"MostFrequentWords");
        j.setJarByClass(MostFrequentWords.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(Text.class);
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j,new Path(args[0]));
        FileOutputFormat.setOutputPath(j,new Path(args[1]));
        System.exit(j.waitForCompletion(true)?0:1);
    }
}

```

#### Execution Commands

```
nano MostFrequentWords.java
```

Save the Java code, then:

```
javac -cp "$(hadoop classpath)" -d . MostFrequentWords.java
jar cf MostFrequentWords.jar MostFrequentWords*.class
```

Run using the previous Word Count output:

```
hadoop jar MostFrequentWords.jar MostFrequentWords /user/cloudera/output /user/cloudera/output_mostfreq
```

View result:

```
hadoop fs -cat /user/cloudera/output_mostfreq/part-r-00000
```

### 6. Expected Output

```
1   bigdata
2   hdfs
2   mapreduce
3   hadoop
```

The words with frequency 2 may appear in either order.

Important: The most frequent word here is `hadoop`, with frequency 3. It appears last because the program sorts frequencies in ascending order, matching the approach shown in your record.

# UNIT III – Apache Hive

## Experiment 3: Identifying Properties of a Dataset using Hive

REFERENCE: BDA RECORD PAGES 8–10

### 1. Aim

To create a Hive table, extract properties such as age, state and gender, and calculate the minimum value, maximum value, number of unique values and count of records per state.

### 2. System Requirements

Hardware: Computer with sufficient RAM and storage.

Software: Linux, Hadoop, HDFS, Apache Hive, Java and terminal.

### 3. Theory

1. Apache Hive is a data warehouse tool built on Hadoop that uses SQL-like queries called HiveQL.
2. Hive is used to analyze large datasets stored in HDFS.
3. An external table refers to data stored at a specified HDFS location.
4. `regexp_extract()` extracts specific values from strings using regular expressions.
5. `MIN()` and `MAX()` find the smallest and largest values.
6. `COUNT(DISTINCT)` calculates the number of unique values.
7. `GROUP BY` groups records based on a common attribute, such as state.

### 4. Procedure

1. Start Hadoop and open the Linux terminal.
2. Create a sample dataset containing date, user ID and properties.
3. Upload the dataset into HDFS.
4. Open Hive and create a database.
5. Create an external Hive table and display the records.
6. Extract age, state and gender using `regexp_extract()`.
7. Calculate minimum, maximum and distinct property values.
8. Use `GROUP BY` to count records for each state.

### 5. Program Code

#### Step 1: Create the input file in Linux

```
nano userdata.csv
```

Enter the following data:

```
2025-11-03,User1,Age=21;state=CA;gender=M;
2025-11-03,User2,Age=25;state=TX;gender=F;
2025-11-03,User3,Age=32;state=CA;gender=M;
2025-11-03,User4,Age=18;state=NY;gender=F;
2025-11-03,User5,Age=40;state=TX;gender=M;
```

Save using `Ctrl + O`, `Enter`, `Ctrl + X`.

#### Step 2: Upload file to HDFS

```
hadoop fs -mkdir -p /user/cloudera/hive_lab/input

hadoop fs -put userdata.csv /user/cloudera/hive_lab/input/
```

#### Step 3: Enter Hive

```
hive
```

#### Step 4: Create database and external table

```

CREATE DATABASE IF NOT EXISTS user_analysis;
USE user_analysis;

CREATE EXTERNAL TABLE IF NOT EXISTS userdata (
    data_date STRING,
    user_id STRING,
    properties STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/hive_lab/input';

SELECT * FROM userdata;

```

#### Step 5: Extract age, state and gender

```

CREATE TABLE userdata_parsed AS
SELECT
    data_date,
    user_id,
    regexp_extract(properties,'Age=([0-9]+)',1) AS age,
    regexp_extract(properties,'state=([A-Z]+)',1) AS state,
    regexp_extract(properties,'gender=([A-Z]+)',1) AS gender
FROM userdata;

SELECT * FROM userdata_parsed;

```

This is the parsing operation shown in your BDA record.

#### Step 6: Find minimum, maximum and unique values

Your syllabus explicitly asks for minimum, maximum and unique-property calculations. The PDF shows the extraction and state-count queries, but not a complete query for these statistics.

The following additional queries satisfy that syllabus requirement.

Minimum, maximum and unique ages

```
SELECT
    MIN(CAST(age AS INT)) AS min_value,
    MAX(CAST(age AS INT)) AS max_value,
    COUNT(DISTINCT age) AS unique_values
FROM userdata_parsed;
```

Why `CAST(age AS INT)`? Because `regexp_extract()` returns text, and we need numerical minimum and maximum values for age.

Unique states

```
SELECT COUNT(DISTINCT state)
FROM userdata_parsed;
```

Unique genders

```
SELECT COUNT(DISTINCT gender)
FROM userdata_parsed;
```

If the examiner specifically asks for the minimum, maximum and distinct-value count for each extracted property, use:

```
SELECT 'age' AS property,
       CAST(MIN(CAST(age AS INT)) AS STRING) AS min_value,
       CAST(MAX(CAST(age AS INT)) AS STRING) AS max_value,
       COUNT(DISTINCT age) AS unique_values
FROM userdata_parsed
UNION ALL
SELECT 'state', MIN(state), MAX(state), COUNT(DISTINCT state)
FROM userdata_parsed
UNION ALL
SELECT 'gender', MIN(gender), MAX(gender), COUNT(DISTINCT gender)
FROM userdata_parsed;
```

For strings, `MIN` and `MAX` use lexical (alphabetical) ordering.

#### Step 7: Generate count per state

```
SELECT state, COUNT(*) AS user_count
FROM userdata_parsed
WHERE state IS NOT NULL AND state <> ''
GROUP BY state;
```

### 6. Expected Output

A. Parsed table

| user_id | age | state | gender |
| ------- | --- | ----- | ------ |
| User1   | 21  | CA    | M      |
| User2   | 25  | TX    | F      |
| User3   | 32  | CA    | M      |
| User4   | 18  | NY    | F      |
| User5   | 40  | TX    | M      |

B. Age statistics

| min_value | max_value | unique_values |
| --------- | --------- | ------------- |
| 18        | 40        | 5             |

C. Statistics for all properties

| property | min_value | max_value | unique_values |
| -------- | --------- | --------- | ------------- |
| age      | 18        | 40        | 5             |
| state    | CA        | TX        | 3             |
| gender   | F         | M         | 2             |

D. Count per state

| State | Count |
| ----- | ----- |
| CA    | 2     |
| TX    | 2     |
| NY    | 1     |


# UNIT III – PIG

## Experiment: Simple Logs Processing using Pig Latin

### 1. Aim

To process log files using Apache Pig and calculate total page views per hour, total views per day, and counts of each hour across all days.

### 2. System Requirements

Hardware: Computer with sufficient RAM and storage.

Software: Linux, Hadoop (HDFS), Apache Pig, terminal or Grunt shell.

### 3. Theory

1. Apache Pig is a Hadoop-based data-processing tool used to analyze large datasets.
2. Pig uses a high-level scripting language called Pig Latin.
3. `LOAD` reads data from HDFS.
4. `GROUP` groups records based on selected fields.
5. `SUM` calculates total page views, while `COUNT` counts records.
6. `DUMP` displays the results.

### 4. Procedure

1. Start Hadoop and open the Linux terminal.
2. Create a sample log file containing site, hour, page views and date.
3. Upload the file into the HDFS directory `/data/pig/simple_logs`.
4. Open Pig using the `pig` command.
5. Load the file and group the data by the required fields.
6. Use `SUM` and `COUNT` for calculations.
7. Display the results using `DUMP`.

### 5. Program Code

Step 1: Create the input file in Linux

```
cat > logs.txt <<'EOF'
A	10	100	2026-10-28
B	10	50	2026-10-28
A	11	80	2026-10-28
A	10	40	2026-10-29
B	11	70	2026-10-29
EOF
```

The four columns are `site`, `hour_of_day`, `page_views` and `data_date`, separated by tab characters.

Step 2: Upload to HDFS

```
hadoop fs -mkdir -p /data/pig/simple_logs
hadoop fs -put logs.txt /data/pig/simple_logs/
```

Step 3: Open Pig

```
pig
```

Step 4: Execute the Pig Latin script

```

A = LOAD '/data/pig/simple_logs/logs.txt'
USING PigStorage('\t')
AS (site:chararray, hour:int, views:int, dt:chararray);

-- i. Total views per hour per day
B = GROUP A BY (dt,hour);
C = FOREACH B GENERATE FLATTEN(group), SUM(A.views);
DUMP C;

-- ii. Total views per day
D = GROUP A BY dt;
E = FOREACH D GENERATE group, SUM(A.views);
DUMP E;

-- iii. Count of each hour across all days
F = GROUP A BY hour;
G = FOREACH F GENERATE group, COUNT(A);
DUMP G;

```

Important: Your syllabus says the input is tab-delimited but also specifies `PigStorage('\u0001')`. Those are different delimiters: `\t` is tab, whereas `\u0001` is the control-A character. The program above uses `\t` because the sample data is genuinely tab-separated. If your lab input uses control-A delimiters, replace `'\t'` with `'\u0001'`.

### 6. Expected Output

i. Total views per hour per day

```
(2026-10-28,10,150)
(2026-10-28,11,80)
(2026-10-29,10,40)
(2026-10-29,11,70)
```

ii. Total views per day

```
(2026-10-28,230)
(2026-10-29,110)
```

iii. Count of each hour across all days

```
(10,3)
(11,2)
```

The order of rows may differ because Pig does not automatically sort the results.

### How to remember this code

There are only three patterns to learn:

```
GROUP A BY (dt,hour) → SUM(A.views)
GROUP A BY dt        → SUM(A.views)
GROUP A BY hour      → COUNT(A)
```

`SUM` adds page views. `COUNT` counts log entries.

## Experiment: Word Count using Pig Latin

### 1. Aim

To count the number of occurrences of each word in a text file using Pig Latin.

### 2. System Requirements

Linux, Hadoop, Apache Pig and terminal.

### 3. Theory

1. Pig Latin is used to process large datasets.
2. `LOAD` reads the input file.
3. `TOKENIZE` splits each line into words.
4. `FLATTEN` converts the words into individual records.
5. `GROUP` groups identical words.
6. `COUNT` calculates the frequency of each word.

### 4. Procedure

1. Create a text file containing words.
2. Upload the file to HDFS.
3. Open the Pig Grunt shell.
4. Load the input file and split the lines into words.
5. Group identical words and count them.
6. Display the result.

### 5. Program Code

Create and upload the text file in Linux:

```
echo "hadoop hive hadoop pig hive" > words.txt
hadoop fs -mkdir -p /data/pig/words
hadoop fs -put words.txt /data/pig/words/
pig
```

Inside Pig:

```

A = LOAD '/data/pig/words/words.txt' AS (line:chararray);
B = FOREACH A GENERATE FLATTEN(TOKENIZE(line)) AS word;
C = GROUP B BY word;
D = FOREACH C GENERATE group, COUNT(B);
DUMP D;

```

### 6. Expected Output

```
(hadoop,2)
(hive,2)
(pig,1)
```

Remember: `LOAD → TOKENIZE → GROUP → COUNT → DUMP`.

# UNIT IV – SQOOP

## Experiment: Hive and MySQL Integration using Sqoop

REFERENCE: BDA RECORD, PAGES 11–21

### 1. Aim

To create a Hive table, import MySQL data into Hive using Sqoop, and export Hive data to the local file system and MySQL.

### 2. System Requirements

Linux, Hadoop, HDFS, MySQL, Apache Hive, Apache Sqoop and Java.

### 3. Theory

1. Sqoop is a Hadoop tool used to transfer structured data between MySQL and Hadoop.
2. `sqoop import` transfers data from MySQL into HDFS or Hive.
3. `sqoop export` transfers data from HDFS into MySQL.
4. Apache Hive allows querying Hadoop data using HiveQL.
5. `--hive-import` imports relational data into Hive.
6. `INSERT OVERWRITE LOCAL DIRECTORY` writes Hive data into a local directory.

### 4. Procedure

1. Open MySQL and create a database and table.
2. Insert sample records into MySQL.
3. Create a database in Hive.
4. Use Sqoop to import MySQL records into Hive.
5. Verify the records in Hive.
6. Export Hive records to a local directory and upload them to HDFS.
7. Use Sqoop export to transfer the HDFS records back into MySQL.

### 5. Program Code

Step 1: Create table in MySQL

Enter MySQL:

```
mysql -u root -p
```

```
CREATE DATABASE IF NOT EXISTS sales;
USE sales;

CREATE TABLE customer (
    id INT,
    name VARCHAR(20)
);

INSERT INTO customer VALUES
(1,'Ram'),(2,'Ravi'),(3,'Anu');

SELECT * FROM customer;
```

Exit MySQL:

```
exit;
```

Step 2: Create Hive database

```
hive
```

```
CREATE DATABASE IF NOT EXISTS labdb;
USE labdb;
```

Exit Hive:

```
exit;
```

Step 3: Import MySQL table into Hive

Execute in Linux:

```
sqoop import \
--connect jdbc:mysql://localhost/sales \
--username root --password cloudera \
--table customer \
--hive-import \
--hive-database labdb \
--hive-table customer \
--m 1
```

This lets Sqoop create the Hive table automatically during the first import.

Step 4: Verify in Hive

```
hive
```

```
USE labdb;
SELECT * FROM customer;
```

Step 5: Export Hive data to local system

Inside Hive:

```
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/export'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM customer;
```

Exit Hive:

```
exit;
```

Step 6: Upload exported data to HDFS

```
hadoop fs -mkdir -p /user/cloudera/export
hadoop fs -put /home/cloudera/export/* /user/cloudera/export/
```

Step 7: Create destination MySQL table

```
mysql -u root -p
```

```
USE sales;
CREATE TABLE customer_new (id INT, name VARCHAR(20));
exit;
```

Step 8: Export from HDFS to MySQL

```
sqoop export \
--connect jdbc:mysql://localhost/sales \
--username root --password cloudera \
--table customer_new \
--export-dir /user/cloudera/export \
--input-fields-terminated-by ',' \
--m 1
```

Step 9: Verify in MySQL

```
mysql -u root -p
```

```
USE sales;
SELECT * FROM customer_new;
```

### 6. Expected Output

| id | name |
| -- | ---- |
| 1  | Ram  |
| 2  | Ravi |
| 3  | Anu  |

The same data should be available in MySQL, Hive and the exported file.

For memorization, focus on these four commands:

- `CREATE TABLE` – Create the MySQL/Hive table.
- `sqoop import --hive-import` – MySQL to Hive.
- `INSERT OVERWRITE LOCAL DIRECTORY` – Hive to local.
- `sqoop export` – HDFS to MySQL.

The code is a short practice version of the workflow shown in your PDF, with simplified customer records. It assumes an empty destination, a configured Cloudera environment and the sample MySQL password `cloudera`. Existing table data or previously generated files can affect reruns.

# UNIT V – DATA VISUALIZATION

## Experiment: Data Visualization Techniques using Python

REFERENCE: BDA RECORD, PAGES 22–25

### 1. Aim

To implement data visualization techniques such as Histogram, Box Plot, Scatter Plot with Regression Line, Heatmap and 3D Scatter Plot using Python.

### 2. System Requirements

Python 3, Jupyter Notebook or Google Colab, Pandas, Matplotlib, Seaborn, Plotly and Scikit-learn.

### 3. Theory

1. Data visualization represents data using graphs and charts to identify patterns and trends.
2. Histogram shows the frequency distribution of numerical data.
3. Box Plot displays the median, quartiles, spread and outliers.
4. Scatter Plot shows relationships between two numerical variables; a regression line shows the trend.
5. Heatmap uses colors to represent correlations between numerical variables.
6. 3D Scatter Plot displays the relationship between three numerical variables.

Your PDF uses the Iris dataset, which contains measurements of different Iris flowers.

[Let ChatGPT Teach You How to Plot with Python and Matplotlib | by Sina Nazeri | The Power of AI | Medium](https://images.openai.com/static-rsc-4/SKEgqalHfhgT6Or_bZ8j6e1KuyurVMXEAdjpX6eLIH4BEnKxcwl1IXCMY5PVWkcGAOeDBDo1ip7r7Gdu-9a8LrrBsNZ43BEiFINdzn-dL_vP_EUXRN-8BKHdcZI8joPP3ZphsNa17N1j1lh3fI06tymsBYICD3jD6XqYYNEIucU?purpose=inline)

Histogram

[matplotlib - In Python's Seaborn, is there any way to do the opposite of \`despine\`? - Stack Overflow](https://images.openai.com/static-rsc-4/w4kzYSPkFEhYZL-9t0NkVxaGjtvQsTwukJ-ApT_VR4r0fct8d8kXXdvXCRdW5q_I3ZSdlXOWrhrpAFHuLgEPgZk6db6pfKUSheqbFMadQaMqZJWYG8NaxlHuflznCuI3QOUeQq59CqTsREeU0LlrxnygZzUrWQZAx_hghbFX9x4?purpose=inline)

Box Plot

[Simple Linear Regression with SciPy | CodeSignal Learn](https://images.openai.com/static-rsc-4/6opiwYnhm3voKDNWzOUwGa4fFTFPiNcr_M6d8zNlFyIigZtiWw2Aqxj_WH_wlFJbTptLnEQhTk2dDMGeldj3g-cEjA6IMqxwHTxYxNs7Kw4xPpnSPPZZAGwjO0wLRN7D3MIZtXKGUWwvWw-PYglxQKha8xOgIyN-cWaDB2T2O0I?purpose=inline)

Scatter Plot with Regression

[Exploratory Data Analysis on the Iris Flower Dataset - DEV Community](https://images.openai.com/static-rsc-4/MGY_SQK2PXJYI_vtUFAf8K1tUcpIYxwtsEAK6xOpl5ScBRCQtebbGJZ_iLcR-pGWSZihe0CKos1CAMzprKm69ZZ1EqyC7MyncSI28g6a_Y7SpTMb1OkBwRwBv5cofFCiA7xRp3Sb2IJ7cDl7L1w2AGonEQok-kdlUCEdaMYeSU4?purpose=inline)

Correlation Heatmap

### 4. Procedure

1. Open Google Colab or Jupyter Notebook.
2. Install and import the visualization libraries.
3. Load the Iris dataset.
4. Create the five visualizations using Python.
5. Add suitable axis labels and titles.
6. Display and analyze the graphs.

### 5. Program Code

Install libraries in Google Colab if necessary:

```
!pip install pandas matplotlib seaborn plotly scikit-learn
```

Single short program for all five visualizations:

```
import pandas as pdimport seaborn as snsimport matplotlib.pyplot as pltimport plotly.express as pxdf = sns.load_dataset("iris")print(df.head())# 1. Histogramsns.histplot(df["sepal_length"])plt.title("Histogram")plt.show()# 2. Box Plotsns.boxplot(data=df, x="species", y="sepal_length")plt.title("Box Plot")plt.show()# 3. Scatter Plot with Regressionsns.regplot(data=df, x="sepal_length", y="petal_length")plt.title("Regression Plot")plt.show()# 4. Heatmapsns.heatmap(df.select_dtypes("number").corr(), annot=True)plt.title("Heatmap")plt.show()# 5. 3D Scatter Plotfig = px.scatter_3d(df, x="sepal_length",                    y="sepal_width",                    z="petal_length",                    color="species")fig.show()
```

### 6. Expected Output

The program generates:

1. A histogram of sepal lengths.
2. A box plot comparing sepal lengths across species.
3. A scatter plot showing the relationship between sepal length and petal length, with a regression line.
4. A heatmap showing correlation coefficients between numerical flower measurements.
5. An interactive 3D scatter plot displaying flower measurements by species.

### Short memory guide

| Graph                | Function to remember |
| -------------------- | -------------------- |
| Histogram            | `sns.histplot()`     |
| Box Plot             | `sns.boxplot()`      |
| Scatter + Regression | `sns.regplot()`      |
| Heatmap              | `sns.heatmap()`      |
| 3D Plot              | `px.scatter_3d()`    |

You only need to remember one dataset-loading command and these five plotting functions.

# UNIT V – MAHOUT MACHINE LEARNING

## Experiment: K-Means Clustering using PySpark MLlib (Mahout-style Workflow)

REFERENCE: BDA RECORD, PAGES 25–29

Your PDF describes a Mahout-style K-Means workflow implemented using PySpark MLlib in Google Colab, rather than providing an Apache Mahout program. We'll follow that approach.

### 1. Aim

To implement K-Means clustering using PySpark MLlib to group similar data points and evaluate the clustering results.

### 2. System Requirements

Google Colab or Jupyter Notebook, Python, Java, PySpark, Pandas and Matplotlib.

### 3. Theory

1. Apache Mahout is a machine learning framework designed for scalable processing of large datasets.
2. PySpark MLlib provides machine learning algorithms for distributed data processing.
3. K-Means is an unsupervised algorithm used to divide data into K clusters.
4. Each cluster has a centroid, representing its center.
5. `VectorAssembler` combines numerical columns into a feature vector.
6. `ClusteringEvaluator` computes the Silhouette score to measure clustering quality. A higher score generally indicates better-separated clusters.

### 4. Procedure

1. Open Google Colab and install PySpark.
2. Initialize the Spark session.
3. Load the Iris dataset.
4. Select numerical features and combine them using `VectorAssembler`.
5. Apply K-Means clustering with `k=3`.
6. Evaluate using the Silhouette score.
7. Display the predicted clusters and cluster centers.

### 5. Program Code

First install PySpark in Google Colab:

```
!pip install pyspark scikit-learn
```

Then execute:

```
from pyspark.sql import SparkSessionfrom pyspark.ml.feature import VectorAssemblerfrom pyspark.ml.clustering import KMeansfrom pyspark.ml.evaluation import ClusteringEvaluatorfrom sklearn.datasets import load_irisspark = SparkSession.builder.appName("KMeans").getOrCreate()# Load Iris datairis = load_iris()cols = list(iris.feature_names)df = spark.createDataFrame(    [tuple(map(float, row)) for row in iris.data],    cols)# Combine featuresdata = VectorAssembler(    inputCols=cols,    outputCol="features").transform(df)# Apply K-Meansmodel = KMeans(k=3, seed=42).fit(data)result = model.transform(data)# Display resultsresult.select("prediction").show(5)print("Silhouette Score:",      ClusteringEvaluator().evaluate(result))print("Cluster Centers:", model.clusterCenters())
```

### 6. Expected Output

The program displays five predicted cluster labels, three cluster center vectors and a Silhouette score.

Example output structure:

```
+----------+
|prediction|
+----------+
|         1|
|         1|
|         1|
|         1|
|         1|
+----------+

Silhouette Score: <calculated value>
Cluster Centers: [center1, center2, center3]
```

The actual cluster labels, center values and score are calculated by the program; they should not be memorized as fixed values.


