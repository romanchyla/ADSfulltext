"""
Integration test for a bibcode that has had no full text extracted before
"""

__author__ = 'J. Elliott'
__maintainer__ = 'J. Elliott'
__copyright__ = 'Copyright 2015'
__version__ = '1.0'
__email__ = 'ads@cfa.harvard.edu'
__status__ = 'Production'
__license__ = 'GPLv3'

import sys
import os

PROJECT_HOME = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)

from lib.test_base import *


class TestExtractWorker(TestGeneric):
    """
    Class that tests the scenario in which the bibcode has no files ever
    extracted to the disk before.
    """

    def setUp(self):
        """
        Generic set up of the class. It first calls the generic super class's
        set up. Following that it creates a link to the all.links file. Finally,
        it determines the expected paths and files that will be generated from
        the tests.

        :return: no return
        """

        super(TestExtractWorker, self).setUp()

        self.test_publish = os.path.join(
            PROJ_HOME,
            'tests/test_integration/stub_data/fulltext_single_document.links'
        )
        self.expected_paths = self.calculate_expected_folders(self.test_publish)

    def tearDown(self):
        """
        Generic tear down of the class. It removes the files and paths created
        in the test. It then calls the super class's tear down afterwards.

        :return: no return
        """

        self.clean_up_path(self.expected_paths)
        #
        # if os.path.exists(self.meta_path):
        #     os.remove(os.path.join(self.meta_path, 'fulltext.txt'))
        #     os.remove(os.path.join(self.meta_path, 'meta.json'))
        #     os.rmdir(self.meta_path)

        super(TestExtractWorker, self).tearDown()

    def test_extraction_of_non_extracted(self):
        """
        Tests the scenario that the bibcode received has had no full text
        extracted for it before. The worker will then extract the full text and
        write it to disk.

        :return: no return
        """

        # user loads the list of full text files and publishes them to the first
        #  queue
        records = read_links_from_file(self.test_publish)

        self.helper_get_details(self.test_publish)
        self.assertEqual(len(records.bibcode),
                         self.nor,
                         'The number of records should match the number of '
                         'lines. It does not: {0:d} [{1:d}]'
                         .format(len(records.bibcode), self.nor))

        # The pipeline converts the input into a payload expected by the workers
        records.make_payload()
        self.assertTrue(len(records.payload)>0)

        # External worker publishes the payload created before to the RabbitMQ
        # queue for the workers to start consuming
        ret = publish(self.publish_worker,
                      records.payload,
                      exchange='FulltextExtractionExchange',
                      routing_key='CheckIfExtractRoute')
        self.assertTrue(ret)
        time.sleep(10)

        # Worker receives packet of information and checks to see if it needs to
        #  be updated
        # see: http://stackoverflow.com/questions/22061082/\
        # getting-pika-exceptions-connectionclosed-error-while-using-rabbitmq
        # -in-python
        print('starting check worker...')
        self.check_worker.run()

        # We pause to simulate the asynchronous running of the workers. This is
        # not needed when the workers
        # are listening continuously.
        time.sleep(10)

        # Check to see if the correct number of updates got published to the
        # next queue
        # Re-declare the queue with passive flag
        standard_queue = self.check_worker.channel.queue_declare(
            queue="StandardFileExtractorQueue",
            passive=True
            )

        pdf_queue = self.check_worker.channel.queue_declare(
            queue="PDFFileExtractorQueue",
            passive=True
            )

        self.assertTrue(standard_queue.method.message_count ==
                        self.number_of_standard_files,
                        'Standard queue should have at least'
                        ' {0:d} message, but it has: {1:d}'
                        .format(self.number_of_standard_files,
                                standard_queue.method.message_count)
                        )
        self.assertTrue(pdf_queue.method.message_count == self.number_of_PDFs,
                        'PDF queue should have at least '
                        '{0:d} message, but it has: {1:d}'
                        .format(self.number_of_PDFs,
                                pdf_queue.method.message_count)
                        )

        # Double check with the worker output
        pdf_res = json.loads(self.check_worker.results['PDF'])
        standard_res = json.loads(self.check_worker.results['Standard'])

        self.assertTrue('NOT_EXTRACTED_BEFORE',
                        'This should be NOT_EXTRACTED_BEFORE, '
                        'but is in fact: {0}'
                        .format(standard_res[0][CONSTANTS['UPDATE']]))

        if pdf_res:
            pdf_res = len(pdf_res)
        else:
            pdf_res = 0

        self.assertEqual(pdf_res,
                         self.number_of_PDFs,
                         'Expected number of PDFs: {0:d}'
                         .format(self.number_of_PDFs))

        self.assertEqual(len(standard_res),
                         self.number_of_standard_files,
                         'Expected number of normal formats: {0:d}'
                         .format(self.number_of_standard_files))

        # There should be no errors at this stage
        queue_error = self.check_worker.channel.queue_declare(
            queue="ErrorHandlerQueue",
            passive=True
            )
        self.assertTrue(queue_error.method.message_count == 0,
                        'Should be 0, but it is: {0:d}'
                        .format(queue_error.method.message_count))

        # Now the next worker collects the list of files that need to be
        # extracted. The Standard Extractor should extract the content of the
        # given payload and so the number of outputs should match the number
        # before. Given we don't expect any errors here!
        print('starting extractor worker')
        self.standard_worker.run()
        number_of_standard_files_2 = \
            len(json.loads(self.standard_worker.results))
        self.assertTrue(number_of_standard_files_2,
                        self.number_of_standard_files)

        # After the extractor, the meta writer should write all the payloads to
        # disk in the correct
        # folders
        print('starting meta writer...')
        self.meta_writer.run()

        time.sleep(5)

        for path in self.expected_paths:
            self.assertTrue(
                os.path.exists(os.path.join(path, 'meta.json')),
                'Meta file not created: {0}'.format(path)
            )
            self.assertTrue(
                os.path.exists(os.path.join(path, 'fulltext.txt')),
                'Full text file not created: {0}'.format(path)
            )


if __name__ == '__main__':
    unittest.main()