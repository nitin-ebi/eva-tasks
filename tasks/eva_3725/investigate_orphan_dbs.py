import argparse
from functools import cached_property

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

logger = logging_config.get_logger(__name__)
logging_config.add_stderr_handler()


class DBInvestigator:

    def __init__(self, maven_config, maven_profile, dbname):
        self.maven_config = maven_config
        self.maven_profile = maven_profile
        self.dbname = dbname

    @cached_property
    def metadata_conn(self):
        return get_metadata_connection_handle(self.maven_profile, self.maven_config)

    @cached_property
    def mongo_conn(self):
        return get_mongo_connection_handle(self.maven_profile, self.maven_config)

    @cached_property
    def mongo_database(self):
        return self.mongo_conn[self.dbname]

    def investigate(self):
        analyses = set()
        for study, analysis in self.mongo_database['files_2_0'].find({},{'fid':1,'sid':1}):
            analyses.add(analysis)

        query = (
            f'SELECT analysis_accession, vcf_reference_accession, hidden_in_eva, a.assembly_set_id, t.taxonomy_id, assembly_code, taxonomy_code '
            f'FROM analysis a '
            f'LEFT OUTER join assembly_set aset ON a.assembly_set_id=aset.assembly_set_id '
            f'LEFT OUTER JOIN taxonomy t ON aset.taxonomy_id=t.taxonomy_id '
            f'WHERE analysis_accession IN {tuple(analyses)}')
        results = get_all_results_for_query(query, self.metadata_conn)
        for (analysis_accession, vcf_reference_accession, hidden_in_eva, assembly_set_id,
            taxonomy_id, assembly_code, taxonomy_code) in results:
            print(analysis_accession, vcf_reference_accession, hidden_in_eva, assembly_set_id,
            taxonomy_id, assembly_code, taxonomy_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Provide the processing status of the EVA projects')
    parser.add_argument("--database",  nargs='*', default=None, help="list of database to investigate",)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", choices=('localhost', 'development', 'production_processing'),
                        help="Profile to look for information", required=True)

    args = parser.parse_args()
    for database_name in args.databases:
        DBInvestigator(args.private_config_xml_file, args.profile, database_name).investigate()
