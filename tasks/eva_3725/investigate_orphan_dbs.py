import argparse
from functools import cached_property

from ebi_eva_common_pyutils.common_utils import pretty_print
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

    def investigate(self):
        analyses, assemblies = self.find_assembly_set_through_analysis()
        files, assembly_set_id_from_browsable_file = self.find_browsable_files_through_analysis()
        reason = None
        assembly_set_id = None
        taxonomy_id = None
        assembly_code = None
        taxonomy_code = None
        if len(analyses) == 0:
            reason = f"1. {self.dbname}: No analysis found in EVAPRO and size is {self.mongo_database_size()} most likely the database was never used ans should be deleted"
        elif len(assemblies) == 0:
            reason = f"2. {self.dbname}-{analyses}: No assembly associated with the analysis. Analysis has been removed from metadata most likely due to deprecation"
        elif len(assemblies) > 1:
            reason = f"3. {self.dbname}-{analyses}: {assemblies}. Multiple assemblies found: mixture of case 4-9"
        else:
            assembly_set_id, taxonomy_id, assembly_code, taxonomy_code = assemblies.pop()
            if assembly_set_id is None:
                reason = f"4. {self.dbname}-{analyses}: Analysis entries exists in DB but is not associated to any assembly set -> Need to set the assembly set."
            elif len(files) == 0:
                reason = f"5. {self.dbname}-{analyses}: Assembly set found associated with analysis but no files found: Check project {self.projects} was completely loaded."
            elif self.dbname != f'eva_{taxonomy_code}_{assembly_code}':
                reason =  f"6. {self.dbname}-{analyses}: Assembly set found associated with analysis for but resulting dbname 'eva_{taxonomy_code}_{assembly_code}' is different. It most likely has been changed after the load. We will have to merge the database"
            elif assembly_set_id != assembly_set_id_from_browsable_file:
                reason = f"7. {self.dbname}-{analyses}: Assembly set from analysis {assembly_set_id} is different from assembly set from files {assembly_set_id_from_browsable_file}. These needs to be set the same"
            elif all( not loaded for file, loaded, la in files):
                reason = f"8. {self.dbname}-{analyses}: Assembly set found but browsable files {list(fid for fid, l, la in files)} are not marked as loaded"
            elif all(not loaded_assembly for file, loaded, loaded_assembly in files):
                reason = f"9. {self.dbname}-{analyses}: Assembly set found but browsable files {list(fid for fid, l, la in files)} do not have loaded assemblies"
            else:
                reason = f"10. {self.dbname}-{analyses}: No good reason found. Most likely due to underscore in the database name that prevented the accurate deconstruction in taxonomy_code and assembly_code"

        return (self.dbname, analyses, assemblies, assembly_set_id, taxonomy_code, assembly_code, files,
                assembly_set_id_from_browsable_file, reason)

    def find_assembly_set_through_analysis(self):
        assemblies = set()
        if self.analyses:
            analyses_str = ','.join([f"'{a}'" for a in self.analyses])
            query = (
                f'SELECT analysis_accession, vcf_reference_accession, hidden_in_eva, a.assembly_set_id, t.taxonomy_id, assembly_code, taxonomy_code '
                f'FROM analysis a '
                f'LEFT OUTER join assembly_set aset ON a.assembly_set_id=aset.assembly_set_id '
                f'LEFT OUTER JOIN taxonomy t ON aset.taxonomy_id=t.taxonomy_id '
                f'WHERE analysis_accession IN ({analyses_str})')
            results = get_all_results_for_query(self.metadata_conn, query)

            for (analysis_accession, vcf_reference_accession, hidden_in_eva, assembly_set_id,
                taxonomy_id, assembly_code, taxonomy_code) in results:
                assemblies.add((assembly_set_id, taxonomy_id, assembly_code, taxonomy_code))
        return self.analyses, assemblies

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Provide the processing status of the EVA projects')
    parser.add_argument("--databases",  nargs='*', default=None, help="list of database to investigate", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", choices=('localhost', 'development', 'production_processing'),
                        help="Profile to look for information", required=True)

    args = parser.parse_args()
    results = []
    header = ["dbname", "reason", "analyses", "assembly_set_id from analysis", "taxonomy_code", "assembly_code",
              "all_files_loaded", "all_assembly_loaded", "assembly_set_id_from_browsable_file"]
    for database_name in args.databases:
        res = DBInvestigator(args.private_config_xml_file, args.profile, database_name).investigate()
        (dbname, analyses, assemblies, assembly_set_id, taxonomy_code, assembly_code, files,
        assembly_set_id_from_browsable_file, reason) = res
        results.append((
            dbname,
            reason,
            ', '.join(analyses) if analyses else '',
            str(assembly_set_id),
            str(taxonomy_code),
            str(assembly_code),
            str(all(file_loaded for fid, file_loaded, loaded_assembly in files)),
            str(all(loaded_assembly for fid, file_loaded, loaded_assembly in files)),
            str(assembly_set_id_from_browsable_file),
        ))

    pretty_print(header, results)
