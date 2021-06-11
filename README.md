# Data Engineering Coding Challenges

## Problem 1
Python file under
```parse_flat_file\parse_flat_file.py``` takes 3 arguments as shown below

```python parse_flat_file.py input_spec_file_path input_fixed_width_file_path output_csv_file_path```

Have a simple docker built on python3 image
1. place the input file under ./parse_flat_file
1. Build docker image ```Docker build . -t yourtagname```
1. Run docker into shell```docker run -it yourtagname sh```
1. Execute ```python parse_flat_file.py yourinput_spec.json yourinput_flatfil youroutput.csv ```
## Problem 2