import sys
import os
import json

from mock import patch
import unittest
from adsft import app, tasks


class TestWorkers(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = tasks.app.conf['PROJ_HOME']
        self._app = tasks.app
        self.app = app.ADSFulltextCelery('test', local_config=\
            {
            })
        tasks.app = self.app # monkey-patch the app object
    
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.app.close_app()
        tasks.app = self._app


    
    def test_task_check_if_extract(self):
        with patch.object(tasks.task_extract_pdf, 'delay', return_value=None) as task_extract_pdf, \
            patch.object(tasks.task_extract_standard, 'delay', return_value=None) as task_extract_standard:
            
            message = {'bibcode': 'fta', 'provider': 'MNRAS', 
                       'ft_source': '{}/tests/test_integration/stub_data/full_test.txt'.format(self.proj_home)}
            tasks.task_check_if_extract(message)
            self.assertTrue(task_extract_standard.called)
            expected = {'bibcode': 'fta', 'file_format': 'txt', 
                        #'index_date': '2017-06-30T22:45:47.800112Z', 
                        'UPDATE': 'NOT_EXTRACTED_BEFORE', 
                        'meta_path': u'{}/ft/a/meta.json'.format(self.app.conf['FULLTEXT_EXTRACT_PATH']), 
                        'ft_source': '{}/tests/test_integration/stub_data/full_test.txt'.format(self.proj_home), 
                        'provider': 'MNRAS'}
            actual = task_extract_standard.call_args[0][0]
            self.assertDictContainsSubset(expected, actual)
            self.assertTrue('index_date' in actual)
            


            
            

if __name__ == '__main__':
    unittest.main()