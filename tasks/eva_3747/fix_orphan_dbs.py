import argparse
import os
from datetime import datetime
from functools import cached_property

import requests
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query, execute_query
from lxml import etree
from retry import retry

logger = logging_config.get_logger(__name__)
logging_config.add_stderr_handler()

@retry(tries=3, delay=2, backoff=1.2, jitter=(1, 3))
def download_xml_from_ena(ena_url) -> etree.XML:
    """Download and parse XML from ENA"""
    try:  # catches any kind of request error, including non-20X status code
        response = requests.get(ena_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise e
    root = etree.XML(bytes(response.text, encoding='utf-8'))
    return root

class EVAPROFixer:

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

    def mongo_database_size(self):
        return self.mongo_database.command('dbstats').get('dataSize')


    @cached_property
    def _project_and_analysis(self):
        projects = set()
        analyses = set()
        for document in self.mongo_database['files_2_0'].find({}, {'fid': 1, 'sid': 1}):
            projects.add(document['sid'])
            analyses.add(document['fid'])
        if not analyses:
            document = self.mongo_database['variants_2_0'].find_one({}, {'files.fid': 1, 'files.sid': 1})
            if document:
                projects.add(document.get('files')[0].get('sid'))
                analyses.add(document.get('files')[0].get('fid'))
        return projects, analyses

    @cached_property
    def projects(self):
        projects, analyses = self._project_and_analysis
        return projects

    @cached_property
    def analyses(self):
        projects, analyses = self._project_and_analysis
        return analyses

    def fix_all(self):
        self.set_assembly_set_id_in_analysis()
        self.insert_browsable_files()
        self.set_assembly_set_id_in_browsable_file()
        self.set_browsable_files_as_loaded()
        self.set_loaded_assembly_in_browsable_file()

    def find_browsable_files_through_analysis(self):
        files= set()
        assembly_set_ids = set()
        if self.analyses:
            analyses_str = ','.join([f"'{a}'" for a in self.analyses])
            query = (
                'SELECT bf.file_id, bf.assembly_set_id, bf.loaded, bf.loaded_assembly '
                'FROM analysis_file af '
                'JOIN browsable_file bf ON af.file_id=bf.file_id  '
                f'WHERE analysis_accession IN ({analyses_str})'
            )
            results = get_all_results_for_query(self.metadata_conn, query)
            for (file_id, assembly_set_id, loaded, loaded_assembly) in results:
                files.add((file_id, loaded, loaded_assembly))
                assembly_set_ids.add(assembly_set_id)
        if len(assembly_set_ids) == 1:
            assembly_set_id = assembly_set_ids.pop()
        else:
            assembly_set_id = None
        return files, assembly_set_id

    def get_published_date(self, project_accession):
        hold_date = None
        xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{project_accession}')
        attributes = xml_root.xpath('/PROJECT_SET/PROJECT/PROJECT_ATTRIBUTES/PROJECT_ATTRIBUTE')
        for attr in attributes:
            if attr.findall('TAG')[0].text == 'ENA-FIRST-PUBLIC':
                hold_date = attr.findall('VALUE')[0].text
                hold_date = datetime.strptime(hold_date, '%Y-%m-%d')
                break
        return hold_date

    def set_browsable_files_as_loaded(self):
        analyses_str = ','.join([f"'{a}'" for a in self.analyses])
        for project_accession in self.projects:
            release_date = self.get_published_date(project_accession)
            if not release_date:
                logger.error(f'Cannot resolve release_date for {project_accession}')
                continue
            release_update = (
                f"update evapro.browsable_file "
                f"set loaded = true, eva_release = '{release_date.strftime('%Y%m%d')}' "
                f"where project_accession='{project_accession}' and loaded = false "
                f"and file_id in (select bf.file_id from analysis_file af "
                f"  join browsable_file bf on af.file_id=bf.file_id "
                f"  where analysis_accession in ({analyses_str}));"
            )
            execute_query(self.metadata_conn, release_update)

    def insert_browsable_files(self, ):
        for project_accession in self.projects:
            # insert into browsable file table, if files not already there
            files_query = (f"select file_id, ena_submission_file_id,filename,project_accession,assembly_set_id "
                           f"from evapro.browsable_file "
                           f"where project_accession = '{project_accession}';")
            rows_in_table = get_all_results_for_query(self.metadata_conn, files_query)
            find_browsable_files_query = (
                "select file.file_id,ena_submission_file_id,filename,project_accession,assembly_set_id "
                "from (select * from analysis_file af "
                "join analysis a on a.analysis_accession = af.analysis_accession "
                "join project_analysis pa on af.analysis_accession = pa.analysis_accession "
                f"where pa.project_accession = '{project_accession}' ) myfiles "
                "join file on file.file_id = myfiles.file_id where file.file_type ilike 'vcf';"
            )
            rows_expected = get_all_results_for_query(self.metadata_conn, files_query)
            if len(rows_in_table) > 0:
                if set(rows_in_table) == set(rows_expected):
                    logger.info('Browsable files already inserted, skipping')
                else:
                    logger.warning(f'Found {len(rows_in_table)} browsable file rows in the table but they are different '
                                 f'from the expected ones: '
                                 f'{os.linesep + os.linesep.join([str(row) for row in rows_expected])}')
            else:
                logger.info('Inserting browsable files...')
                insert_query = ("insert into browsable_file (file_id,ena_submission_file_id,filename,project_accession,"
                                "assembly_set_id) " + find_browsable_files_query)
                execute_query(self.metadata_conn, insert_query)

    def set_assembly_set_id_in_browsable_file(self):
        for analysis in self.analyses:
            query = (
                f"update browsable_file "
                f"set assembly_set_id=(select assembly_set_id from analysis where analysis_accession='{analysis}') "
                f"where assembly_set_id is NULL and file_id in "
                f"(select bf.file_id from analysis_file af join browsable_file bf on af.file_id=bf.file_id  where af.analysis_accession='{analysis}')"
            )
            execute_query(self.metadata_conn, query)

    def set_loaded_assembly_in_browsable_file(self):
        for analysis in self.analyses:
            query = (
                f"update browsable_file "
                f"set loaded_assembly=(select assembly_accession from assembly ab join analysis an on ab.assembly_set_id=an.assembly_set_id where an.analysis_accession='{analysis}') "
                f"where loaded_assembly is NULL and file_id in "
                f"(select bf.file_id from analysis_file af join browsable_file bf on af.file_id=bf.file_id  where af.analysis_accession='{analysis}')"
            )
            execute_query(self.metadata_conn, query)

    def set_assembly_set_id_in_analysis(self):
        for analysis in self.analyses:
            query = (
                "select p.project_accession, a.vcf_reference_accession, a.assembly_set_id "
                "from project p join project_analysis pa on pa.project_accession=p.project_accession "
                f"join analysis a on pa.analysis_accession=a.analysis_accession where a.analysis_accession='{analysis}';"
            )
            res = list(get_all_results_for_query(self.metadata_conn, query))
            if not res:
                logger.error(f"{analysis} does not exist in EVAPRO")
                continue
            project, assembly_accession, assembly_set_id = res[0]
            if assembly_set_id is None:
                query = f"select taxonomy_id from project_taxonomy where project_accession='{project}';"
                res = list(get_all_results_for_query(self.metadata_conn, query))
                if not res:
                    logger.error(f"{project} does not exist in EVAPRO")
                    continue
                taxonomy_id, = res[0]
                query = f"select assembly_set_id from assembly where assembly_accession='{assembly_accession}' and taxonomy_id={taxonomy_id};"
                res = list(get_all_results_for_query(self.metadata_conn, query))
                if not res:
                    logger.error(f"{assembly_accession} has not been added to EVAPRO")
                    continue
                assembly_set_id, = res[0]
                logger.info(f"Analysis {analysis} has been added to EVAPRO")
                query = f"update  analysis set assembly_set_id={assembly_set_id} where analysis_accession='{analysis}';"
                execute_query(self.metadata_conn, query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix the EVAPRO setup for the provided variant warehouse database')
    parser.add_argument("--databases",  nargs='*', default=None, help="list of database to fix", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", choices=('localhost', 'development', 'production_processing'),
                        help="Profile to look for information", required=True)

    args = parser.parse_args()
    for database_name in args.databases:
        EVAPROFixer(args.private_config_xml_file, args.profile, database_name).fix_all()
