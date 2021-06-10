import json
import os
import sys
from json import JSONDecodeError
from typing import Dict, List


def read_json_spec(file_path: str) -> Dict:
    """

    :param file_path:
    :return:
    """
    ip_spec = None
    try:
        js = open(file=file_path, mode="r")
        ip_spec = json.load(js)
    except IOError as err:
        print(f"Problem opening the spec file\n {str(err)}")
    except JSONDecodeError as err:
        print(f"Problem decoding input spec\n {str(err)}")
    return ip_spec


def parse_line(col_offsets: List[int],
               ip_line: str) -> str:
    """

    :param col_offsets:
    :param ip_line:
    :return:
    """
    op_line = ""
    # loop through the column offsets
    for offset in col_offsets:
        op_line += (ip_line[:offset]).strip() + ","
        ip_line = ip_line[offset:]
    return op_line.strip(",") + os.linesep


def parse_fixed_width_file(spec_file_path: str,
                           ip_flat_file_path: str,
                           op_csv_file_path: str):
    """

    :param spec_file_path:
    :param ip_flat_file_path:
    :param op_csv_file_path:
    :return:
    """
    ip_spec = read_json_spec(spec_file_path)
    try:
        spec_header = ",".join(ip_spec["ColumnNames"]) + os.linesep
        col_offsets = ip_spec["Offsets"]
        # convert char to int
        col_offsets = [int(offset) for offset in col_offsets]
        ip_file_encoding = ip_spec["FixedWidthEncoding"]
        op_header = True if (ip_spec["IncludeHeader"] == "True") else False
        op_encoding = ip_spec["DelimitedEncoding"]
    except Exception as err:
        print(f"error parsing Input Json Spec.\n {str(err)}")

    try:
        op_file = open(file=op_csv_file_path, mode="w", encoding=op_encoding)
        with open(file=ip_flat_file_path, mode="r", encoding=ip_file_encoding) as ip_file:
            # read header
            header_line = ip_file.readline()
            if not header_line:
                sys.exit()
            parsed_header = parse_line(col_offsets=col_offsets, ip_line=header_line)
            if parsed_header == spec_header:
                # if the input file has the header
                if op_header:
                    # if output needs a header
                    op_file.write(parsed_header)
            else:
                # if the input doesn't contain the header
                if op_header:
                    # if output needs header, write it from the spec
                    op_file.write(spec_header)
                # write the read line, as the first line is the data line
                op_file.write(parsed_header)

            while True:
                ip_line = ip_file.readline()
                if not ip_line:
                    break
                op_file.write(parse_line(col_offsets=col_offsets, ip_line=ip_line))
        op_file.close()
    except Exception as err:
        print(f"Error parsing flat file\n{str(err)}")


if len(sys.argv) == 4:
    parse_fixed_width_file(spec_file_path=sys.argv[1],
                           ip_flat_file_path=sys.argv[2],
                           op_csv_file_path=sys.argv[3])
else:
    print(f"insufficient inputs")
    print(f"python parse_flat_file.py input_spec_file_path input_fixed_width_file_path output_csv_file_path")
