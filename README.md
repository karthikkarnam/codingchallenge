# Data Engineering Coding Challenges

## Problem 1: parse_flat_file
Application
```parse_flat_file\parse_flat_file.py``` takes 3 arguments as shown below

```python parse_flat_file.py input_spec_file_path input_fixed_width_file_path output_csv_file_path```

Simple docker built on python3 image
1. Place the input file under ```parse_flat_file/```
1. Build docker image ```Docker build . -t yourtagname```
1. Run docker into shell```docker run -it yourtagname sh```
1. Execute ```python parse_flat_file.py yourinput_spec.json yourinput_flatfil youroutput.csv ```


## Problem 2: anonymise_data
**Using Spark3, and application code in  Python for  on windows**
* Generated a 2 CSV files from generate_csv.py which uses Faker package<br>
  * file with 10,000 records which is 770KB, which is available here<br>
  * file with 27mil records which is 1.97GB 
* Have 2 options to anonymise the data
  * ```anonymise_data_hash.py``` is the quickest, uses hash SQL inbuilt function to generate integers<br>
    (if you don't care about, how the data looks like as long as source data is anonymised)
    ```
    spark-submit --name "anonymise_data_hash" \
                 --master "local[*]" \
                 --conf "spark.driver.cores=4" \
                 --conf "spark.driver.memory=512m" \
                 anonymise_data_hash.py \
                 --input_csv "people_data_large.csv" \
                 --output_csv "anonymised_people_data_large.csv" 
    ```
    (the above run takes 50 secs to anonymise the file which is of 2 GB)
    
  * ```anonymise_data_sql.py``` uses a custom UDF in python which generates random strings, which is a bit expensive<br>
    (Hash functions could be reverse engineered, if you randomise the data there is no possibility of getting back the data)
     ```
    spark-submit --name "anonymise_data_sql" \
                 --master "local[*]" \
                 --conf "spark.driver.cores=4" \
                 --conf "spark.driver.memory=512m" \
                 anonymise_data_sql.py \
                 --input_csv "people_data_large.csv" \
                 --output_csv "anonymised_people_data_large.csv" 
    ```
    (the above run takes 854 secs to anonymise the file which is of 2 GB)
    
**System Configuration**
* 4 physical cores
* 8 GB Memory
* SSD