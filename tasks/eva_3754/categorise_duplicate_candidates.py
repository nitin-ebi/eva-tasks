import datetime
import itertools
import json
import sys
from argparse import ArgumentParser
from collections import Counter


def convert_date(datetime_dict):
    date_tmp = {}
    date_tmp.update(datetime_dict.get('date'))
    date_tmp.update(datetime_dict.get('time'))
    date_tmp['microsecond'] = int(date_tmp.pop('nano')/1000)
    return datetime.datetime(**date_tmp)

def min_time_gap_between_create_date(cve_list):
    date_list = [convert_date(cve.get('createdDate')) for cve in cve_list]
    delta_dates = [abs(d1-d2) for d1,d2 in  itertools.combinations(date_list, 2)]
    min_delta_date = min(delta_dates)
    return min_delta_date

def more_than_cve_with_assembly(cve_list):
    assembly_count = Counter([cve.get('assemblyAccession') for cve in cve_list])

    return ','.join(sorted([assembly for assembly in assembly_count if assembly_count[assembly] > 1]))


def are_taxonomy_consistent(sve_map):
    taxonomy_set_list = []
    for sve_group in sve_map:
        taxonomy_set_list.append(set(sve.get('taxonomy') for sve in sve_map[sve_group]))
    for taxonomy_set1, taxonomy_set2 in itertools.combinations(taxonomy_set_list, 2):
        if not taxonomy_set1.intersection(taxonomy_set2):
            return False
    return True

def eva_or_dbsnp_accession(accession):
    if accession > 5000000000:
        return (accession // 100000) * 100000
    return None

def categorise_from_json(accession, json_doc):
    out = []
    cve_list = json_doc.get('clusteredVariantEntityList')
    sve_map = json_doc.get('submittedVariantEntityMap')
    min_date = min_time_gap_between_create_date(cve_list)

    if min_date < datetime.timedelta(days=1):
        out.append('CREATED_AT_THE_SAME_TIME')
    else:
        out.append('CREATED_SEPARATELY')
    assemblies = more_than_one_assembly(cve_list)
    if assemblies:
        out.append(f'IN_SAME_ASSEMBLIES_{assemblies}')
    else:
        out.append('IN_DIFFERENT_ASSEMBLIES')
    accessioning_block = eva_or_dbsnp_accession(accession)
    if accessioning_block:
        out.append(f'RECENT_ACCESSION_{accessioning_block}')
    else:
        out.append('OLD_ACCESSION')
    if are_taxonomy_consistent(sve_map):
        out.append('CONSISTENT_TAXONOMY')
    else:
        out.append('INCONSISTENT_TAXONOMY')

    return '\t'.join(out)

def parse_line(line):
    sp_line = line.strip().split(' ')
    rsid = sp_line[0]
    json_doc = json.loads(' '.join(sp_line[1:]))
    out = categorise_from_json(accession=int(rsid), json_doc=json_doc)
    print(f'{rsid}\t{out}')

def main():
    arg_parse = ArgumentParser(description='Categorise duplicate candidates')
    arg_parse.add_argument('file', help='files to process')
    args = arg_parse.parse_args()
    with open(args.file) as open_file:
        for line in open_file:
            parse_line(line)

    return 0


if __name__ == '__main__':
    sys.exit(main())
