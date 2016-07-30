# log-parser-exercise

This is a simple log parser created for an excercise. 

## Dependencies

Python Version: 2.7

* pip (8.1.2)
* python-dateutil (2.5.3)
* PyYAML (3.11)
* setuptools (12.0.5)
* six (1.10.0)
* ua-parser (0.7.1)
* user-agents (1.0.1)

## Usage:
```
$ python log_parser.py -h
usage: log_parser.py [-h] [-v] [-d] [-q] -i FILE [-o OUTPUT_FORMAT] [-t TOP_N]
                     [-f] [-u] [-r]

Parse extended NCSA format log files and output stats in various formats

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Print warning messages.
  -d, --debug           Print debug messages and output unparsed report
  -q, --quiet           Do not print status messages.
  -i FILE, --input FILE
                        File to process
  -o OUTPUT_FORMAT, --output_format OUTPUT_FORMAT
                        Output format. Supported: pretty, [yaml], json
  -t TOP_N, --top_n TOP_N
                        Number of most frequent items to list. A setting of
                        '0' skips this output. [0]
  -f, --full            Output full stats.
  -u, --urls            Whether or not to output URL stats. Requires -t > 0
  -r, --get_post_ratios
                        Whether or not to output GET:POST ratios.

```

## Example usage

```
python log_parser.py -i sample_data/sample.log -o yaml -t 5 -q
```

