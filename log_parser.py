#!/usr/bin/env python

import argparse
import copy
from collections import Counter
from dateutil import parser as du_parser
from fractions import gcd
import json
import logging
import operator
import os
import pprint
import re
import sys
import yaml
from user_agents import parse as ua_parse

pp = pprint.PrettyPrinter(indent=4)


def arg_parser():
    parser = argparse.ArgumentParser(description='Parse extended NCSA ' +
                                     'format log files and output stats in ' +
                                     'various formats')
    parser.add_argument("-v", "--verbose",
                        action="store_true", dest="verbose",
                        help="Print warning messages.")
    parser.add_argument("-d", "--debug",
                        action="store_true", dest="debug",
                        help="Print debug messages and output unparsed report")
    parser.add_argument("-q", "--quiet",
                        action="store_true", dest="quiet",
                        help="Do not print status messages.")
    parser.add_argument("-i", "--input", dest="input_file",
                        help="File to process", metavar="FILE", required=True)
    parser.add_argument("-o", "--output_format", dest="output_format",
                        help="Output format. Supported: pretty, [yaml], json",
                        default="yaml")
    parser.add_argument("-t", "--top_n", dest="top_n",
                        help="Number of most frequent items to list. A " +
                             "setting of '0' skips this output. [0] ",
                        default=0)
    parser.add_argument("-f", "--full", action="store_true", dest="full_stats",
                        help="Output full stats.")
    parser.add_argument("-u", "--urls", dest="output_urls",
                        help="Whether or not to output URL stats. " +
                             "Requires -t > 0",
                        action="store_true")
    parser.add_argument("-r", "--get_post_ratios", dest="get_post_ratios",
                        help="Whether or not to output GET:POST ratios.",
                        action="store_true")
    return parser


def main():
    parser = arg_parser()
    args = parser.parse_args()
    # ----------------
    # Parse arguments
    # ----------------
    if not os.path.isfile(args.input_file):
        print "*************************************************"
        print "Input argument does not point to a file or glob"
        print "of files."
        print "*************************************************"
        print parser.print_help()
        sys.exit()

    if args.output_format not in ['pretty', 'json', 'yaml', 'csv']:
        print "********************************************************"
        print "Output must be one of 'pretty', 'json', 'yaml' or 'csv'."
        print "********************************************************"
        print parser.print_help()
        sys.exit()

    if args.output_urls is True and args.top_n < 1:
        print "********************************************************"
        print "'-u' requires '-t' > 0"
        print "********************************************************"
        print parser.print_help()
        sys.exit()

    if args.top_n == 0 and args.full_stats is False and \
            args.output_urls is False and args.get_post_ratios is False:
        print "********************************************************"
        print "Nothing to do: '-t', -f', '-u' and/or '-r' must be specified"
        print "********************************************************"
        print parser.print_help()
        sys.exit()

    # -----------------
    # Configure logging
    # -----------------
    if args.debug is True:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose is True:
        logging.basicConfig(level=logging.WARNING)
    elif args.quiet is True:
        logging.basicConfig(level=logging.CRITICAL)

    # --------------------------
    # Output requested data in
    # requested format.
    # --------------------------
    parsed_logs = LogParser(**vars(args))
    unparsed = {'Unparsed lines': parsed_logs.parsed_file['unparsed']}
    if args.output_format == 'pretty':
        if int(args.full_stats) != 0:
            full_stats = {'Full Stats': parsed_logs.stats}
            pp.pprint(full_stats)
        if int(args.top_n) > 0:
            top_stats = {'Top Stats': parsed_logs.top_stats}
            pp.pprint(top_stats)
        if args.get_post_ratios:
            rat = {'Ratio of GET:POST by Day': parsed_logs.ratio}
            print yaml.safe_dump(rat, default_flow_style=False)
        # Print Error report
        if args.debug:
            pp.pprint(unparsed)
    elif args.output_format == 'json':
        if args.full_stats:
            full_stats = {'Full Stats': parsed_logs.stats}
            print json.dumps(full_stats)
        if int(args.top_n) > 0:
            top_stats = {'Top Stats': parsed_logs.top_stats}
            print json.dumps(top_stats)
        if args.get_post_ratios:
            rat = {'Ratio of GET:POST by Day': parsed_logs.ratio}
            print yaml.safe_dump(rat, default_flow_style=False)
        # Print Error report
        if args.debug:
            print json.dumps(unparsed)
    elif args.output_format == 'yaml':
        if int(args.full_stats) != 0:
            full_stats = {'Full Stats': parsed_logs.stats}
            print yaml.dump(full_stats, default_flow_style=False)
        if int(args.top_n) > 0:
            top_stats = {'Top Stats': parsed_logs.top_stats}
            print yaml.safe_dump(top_stats, default_flow_style=False)
        if args.get_post_ratios:
            rat = {'Ratio of GET:POST by Day': parsed_logs.ratio}
            print yaml.safe_dump(rat, default_flow_style=False)
        # Print Error report
        if args.debug:
            print yaml.safe_dump(unparsed, default_flow_style=False)


# -----------------
# Meat and potatoes
# -----------------
class LogParser:

    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        logging.debug('Setting up data structures...')
        # Hold collected stats in here
        self.t_stat_dict = {'by_day': {}, 'all_time': {}}
        self.t_stats = {'agents': copy.deepcopy(self.t_stat_dict),
                        'os': copy.deepcopy(self.t_stat_dict),
                        'requests': copy.deepcopy(self.t_stat_dict),
                        'request_type': copy.deepcopy(self.t_stat_dict),
                        'request_proto': copy.deepcopy(self.t_stat_dict),
                        'request_url': copy.deepcopy(self.t_stat_dict)
                        }
        # Log line format regex
        self.line_parts_re = [r'(?P<host>\S+)',                    # host
                              r'\S+',                              # indent
                              r'(?P<user>\S+)',                    # user
                              r'\[(?P<datetime>.+)\]',             # datetime
                              r'"(?P<request_type>[A-Z]{3,7})',    # req type
                              r'(?P<request_url>\S+)',             # req url
                              r'(?P<request_proto>.+)"',           # req proto
                              r'(?P<status>[0-9]+)',               # status
                              r'(?P<size>\d+|\-)',                 # size
                              r'"(?P<referer>.*)"',                # referer
                              r'"(?P<agent>.*)"',                  # user agent
                              ]
        logging.debug('Running _parse_file()...')
        self.parsed_file = self._parse_file()
        # Stats
        self.stats = {}
        logging.debug('Running _compile_stats()...')
        self.stats = self._compile_stats()
        # Top n stats
        self.top_stats = {}
        if self.top_n > 0:
            logging.debug('Running _compile_top_n()...')
            self.top_stats = self._compile_top_n()
        # Get:Post ratios
        self.ratio = {}
        if self.get_post_ratios is True:
            logging.debug('Running _compile_ratios_of_os_g_and_p()...')
            self.ratio = self._compile_ratios_of_os_g_and_p()

    def _parse_file(self):
        # Hold parsed data in here
        parsed_file = {'parsed': [], 'unparsed': []}
        # Regex for properly formattted lines
        line_re = re.compile(r'\A'+r'\s+'.join(self.line_parts_re))
        # Loop through file and parse its content
        # Open file
        with open(self.input_file) as f:
            logging.debug('Parsing input file "%s"' % self.input_file)
            # Iterate over lines
            for line in f:
                ml = line_re.match(line.strip())
                if ml is None:
                    parsed_file['unparsed'].append(line)
                else:
                    pl = ml.groupdict()
                    # Update agent with parsed version
                    pl['agent'] = ua_parse(pl['agent'])
                    # Parse timestamp to datetime obj
                    pd = self._convert_times_to_objs(pl['datetime'])
                    # If cannot be parsed move line to unparsed list
                    if pd is None:
                        logging.warning("Unparsable line found. You can " +
                                        "review unparsable lines by running" +
                                        " with the debug flag -d.")
                        parsed_file['unparsed'].append(line)
                    else:
                        pl['datetime'] = pd
                        parsed_file['parsed'].append(pl)
            logging.debug('Input File "%s" ' % self.input_file +
                          'parsed successfully')
            return parsed_file

    def _convert_times_to_objs(self, dt_stamp):
        try:
            # Meh dateutil doesn't like a ':' between date and time
            if dt_stamp[11] == ':':
                tstamp = '%s %s' % (dt_stamp[:11],
                                    dt_stamp[13:])
            else:
                tstamp = dt_stamp
            return du_parser.parse(tstamp)
        except:
            # Garbage in timestamp
            return None

    def _compile_stats(self):
        # Hold collected stats in here
        stat_dict = {'by_day': {}, 'all_time': {}}
        stats = {'agents': copy.deepcopy(stat_dict),
                 'os': copy.deepcopy(stat_dict),
                 'requests': copy.deepcopy(stat_dict),
                 'request_type': copy.deepcopy(stat_dict),
                 'request_proto': copy.deepcopy(stat_dict),
                 'request_url': copy.deepcopy(stat_dict)
                 }
        stats['requests']['all_time'] = 0
        this_day = None
        for line in self.parsed_file['parsed']:
            # It's a new day
            day = str(line['datetime'].date())
            # Reset by_day counters
            if this_day != day:
                this_day = day
                stats['requests']['by_day'][day] = 0
                stats['agents']['by_day'][day] = {}
                stats['request_type']['by_day'][day] = {}
                stats['request_url']['by_day'][day] = {}
                stats['request_proto']['by_day'][day] = {}
                stats['os']['by_day'][day] = {}
            # ------------------
            # Number of requests
            # ------------------
            stats['requests']['all_time'] += 1
            stats['requests']['by_day'][day] += 1
            # ------------------
            # Agents Families
            # ------------------
            browser_family = line['agent'].browser.family
            this_day_agents = stats['agents']['by_day'][day].keys()
            if browser_family not in this_day_agents:
                stats['agents']['by_day'][day][browser_family] = 0
            if browser_family not in stats['agents']['all_time'].keys():
                stats['agents']['all_time'][browser_family] = 0
            stats['agents']['by_day'][day][browser_family] += 1
            stats['agents']['all_time'][browser_family] += 1
            # -------------
            # OS families
            # -------------
            os_family = line['agent'].os.family
            if os_family not in this_day_agents:
                stats['os']['by_day'][day][os_family] = 0
            if os_family not in stats['os']['all_time'].keys():
                stats['os']['all_time'][os_family] = 0
            stats['os']['by_day'][day][os_family] += 1
            stats['os']['all_time'][os_family] += 1
            # ---------------
            # Request type
            # ---------------
            request_type = line['request_type']
            this_day_request_types = stats['request_type'
                                           ]['by_day'][day].keys()
            if request_type not in this_day_request_types:
                stats['request_type']['by_day'][day][request_type] = 0
            if request_type not in stats['request_type'
                                         ]['all_time'].keys():
                stats['request_type']['all_time'][request_type] = 0
            stats['request_type']['by_day'][day][request_type] += 1
            stats['request_type']['all_time'][request_type] += 1
            # -------------
            # Request URLs
            # -------------
            if self.output_urls == 1:
                url = line['request_url']
                this_day_urls = stats['request_url']['by_day'][day].keys()
                if url not in this_day_urls:
                    stats['request_url']['by_day'][day][url] = 0
                if url not in stats['request_url']['all_time'].keys():
                    stats['request_url']['all_time'][url] = 0
                stats['request_url']['by_day'][day][url] += 1
                stats['request_url']['all_time'][url] += 1
            # -------------
            # Request Protos
            # -------------
            proto = line['request_proto']
            this_day_protos = stats['request_proto']['by_day'][day].keys()
            if proto not in this_day_protos:
                stats['request_proto']['by_day'][day][proto] = 0
            if proto not in stats['request_proto']['all_time'].keys():
                stats['request_proto']['all_time'][proto] = 0
            stats['request_proto']['by_day'][day][proto] += 1
            stats['request_proto']['all_time'][proto] += 1
        # ---------------------
        # Delete unwanted stats
        # ---------------------
        if self.output_urls != 1:
            del stats['request_url']
        return stats

    def _compile_top_n(self):
        top_stats = copy.deepcopy(self.t_stats)
        for stat, data in self.stats.iteritems():
            if stat == 'requests':
                continue
            # By Day
            for day, ddata in data['by_day'].iteritems():
                dict_top_sorted_data = {}
                sorted_data = sorted(ddata.iteritems(),
                                     key=operator.itemgetter(1), reverse=True)
                top_sorted_data = sorted_data[:int(self.top_n)]
                for d in top_sorted_data:
                    k, v = d
                    dict_top_sorted_data[k] = v
                top_stats[stat]['by_day'][day] = dict_top_sorted_data
                top_stats[stat]['all_time'] = \
                    dict(Counter(top_stats[stat]['by_day'][day]) +
                         Counter(top_stats[stat]['all_time']))
        # ---------------------
        # Delete unwanted stats
        # ---------------------
        del top_stats['requests']
        if self.output_urls != 1:
            del top_stats['request_url']
        return top_stats

    # Ideally this is turned into a dynamic method where
    # anything can be compared, example:
    # self._compile_ratios_of_x_and_y('os.family', 'request_type',
    #                                 'GET', 'POST')
    def _compile_ratios_of_os_g_and_p(self):
        ratio = {}
        # Set up template dictionary for data
        data_dict = {'count': {'GET': 0, 'POST': 0},
                     'percentage': {'GET': 0.0, 'POST': 0.0},
                     'ratio': ''}
        this_day = None
        for line in self.parsed_file['parsed']:
            day = str(line['datetime'].date())
            # It's a new day
            if this_day != day:
                this_day = day
                # New day dict
                ratio[day] = {}
            os = line['agent'].os.family
            if os not in ratio[day].keys():
                ratio[day][os] = copy.deepcopy(data_dict)
            if line['request_type'] == 'GET':
                ratio[day][os]['count']['GET'] += 1
            if line['request_type'] == 'POST':
                ratio[day][os]['count']['POST'] += 1
        # Calculate ratios
        for day, val in ratio.iteritems():
            for os, data in val.iteritems():
                # Calculate Percentages
                total_req = data['count']['GET'] + data['count']['POST']
                get_perc = int(100 * (float(data['count']['GET']) /
                                      float(total_req)))
                post_perc = int(100 * (float(data['count']['POST']) /
                                       float(total_req)))
                ratio[day][os]['percentage']['GET'] = \
                    "%s" % str(get_perc)
                ratio[day][os]['percentage']['POST'] = \
                    "%s" % str(post_perc)
                # Format as ratio
                ratio_str = "%s:%s" % (str(get_perc), str(post_perc))
                # Simplify
                ratio[day][os]['ratio'] = self._simplify_ratio(ratio_str, ':')
        return ratio

    def _simplify_ratio(self, ratio, delimeter):
        nums = [int(i) for i in ratio.split(delimeter)]
        denoms = reduce(gcd, nums)
        res = [i/denoms for i in nums]
        return delimeter.join(str(i) for i in res)

if __name__ == "__main__":
    main()
