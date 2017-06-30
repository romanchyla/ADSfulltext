"""
Functional test

Loads the ADSfulltext workers for both the Python and Java code. It then injects
a number of full text articles to be extracted onto the RabbitMQ instance. Once
extracted, it checks that the full text file and the meta.json file have been
written to disk. It then shuts down all of the workers.
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

import os
import time
import subprocess
import string
import unittest
import celery
from adsft.tests import test_base
import run

class TestExtractWorker(test_base.TestUnit):
    """
    Class for testing the overall functionality of the ADSfull text pipeline.
    The interaction between the Python workers, Java workers, and the RabbitMQ
    instance.
    """

    def setUp(self):
        """
        Sets up the Python and Java workers using supervisorctl. It then sleeps
        before allowing the rest of the tests to proceed to ensure that the
        workers have started correctly.

        :return: no return
        """
        super(TestExtractWorker, self).setUp()
        self.expected_folders = []
        self._start_pipeline()

    def tearDown(self):
        """
        Tears down the relevant parts after the test has finished. It first
        tries to empty all of the queues. It then tries to delete the relevant
        content that was written to disk during the extraction. Finally, it
        stops the workers using supervisorctl.

        :return: no return
        """
        
        super(TestExtractWorker, self).tearDown()
        self._stop_pipeline()
        

    def helper_make_link(self, outfile):
        """
        Helper function that writes the all.links file. This file contains the
        list of bibcodes, path to file, and provider. This ensures there is no
        hard coding.

        :param outfile: name of file to be made
        :return: no return
        """

        letter_list = string.ascii_lowercase
        with open(
                "{home}/{filename}".format(home=self.app.conf['PROJ_HOME'], filename=outfile),
                'w'
        ) as f_out:

            for letter in string.ascii_lowercase:
                if letter == 'u':
                    break

                f_out.write(
                    'ft{letter}\t'
                    '{home}/tests/test_integration/stub_data/full_test.txt\t'
                    'MNRAS\n'
                    .format(letter=letter, home=self.app.conf['PROJ_HOME'])
                )

                letter_list = letter_list[1:]

            for letter in letter_list:
                f_out.write(
                    'ft{letter}\t'
                    '{home}/src/test/resources/test_doc.pdf\t'
                    'MNRAS\n'
                    .format(letter=letter, home=self.app.conf['PROJ_HOME'])
                )
    
    def _get_rabbitmq_container(self):
        """Retrieves the docker image of the rabbitmq container; if not available
        raise error with instructions."""
        
        output = subprocess.check_output('docker ps -f name=rabbitmq -q'.split())
        if not output or len(output) < 10:
            raise Exception('Cannot find running docker container named: "rabbitmq"' \
                            "Have you done the following?" \
                            " git clone https://github.com/adsabs/devtools " \
                            " cd devtools" \
                            " vagrant up rabbitmq --provider=docker"
                            )
        return output.strip()
        
    def _start_pipeline(self):
        cid = self._get_rabbitmq_container()
        vhost = self.app.conf['CELERY_BROKER'].split('/')[-1]
        
        # create the vhost
        subprocess.call('docker exec {} rabbitmqctl add_vhost {}'
                              .format(cid, vhost), shell=True)
        subprocess.call('docker exec {} rabbitmqctl set_permissions -p {} guest ".*" ".*" ".*"'
                              .format(cid, vhost), shell=True)
        
        # note(rca): I've tried very hard to avoid this concoction but was always thrown back by 
        # errors down the line; so I settled with the stupid shell command that relies on presence
        # of virtualenv and bash
        # 
        pypath = os.path.dirname(sys.executable)
        command = 'cd {proj_home} && celery -D -A adsft.tasks worker -b {celery}'.\
            format(pypath=pypath, celery=self.app.conf['CELERY_BROKER'], proj_home=self.app.conf['PROJ_HOME'])
        if os.path.exists(pypath + '/activate'):
            command = 'source {pypath}/activate && '.format(pypath=pypath) + command 
            
        print 'executing', command
        subprocess.check_call(command, shell=True, executable='/bin/bash')
        # start celery
        #args = 'celery -A adsft.tasks worker -b {}' \
        #                      .format(self.app.conf['CELERY_BROKER'], ).split(' ')
        #print args
        #cc = CeleryCommand()
        #super(CeleryCommand, cc).execute_from_commandline(args)
        
        #x = self.app.start(argv='celery -D -A adsft.tasks worker -b \'{}\''
        #                  .format(self.app.conf['CELERY_BROKER'],).split())
        #subprocess.check_call('{} -m adsft.tasks -D -A adsft.tasks worker -b {}'.
        #                      format(subprocess.check_output('which python', shell=True).strip(), vhost),
        #                      shell=True, env=os.environ.copy())
        #print subprocess.Popen(args, env=os.environ.copy())
        #print subprocess.check_output(' '.join(args), executable='/bin/bash', 
        #                              shell=True, env=os.environ.copy())
        
    
    def _stop_pipeline(self):
        cid = self._get_rabbitmq_container()
        vhost = self.app.conf['CELERY_BROKER'].split('/')[-1]
        
        # stop celery
        pypath = os.path.dirname(sys.executable)
        command = 'cd {proj_home} && celery control shutdown -b {celery}'.\
            format(pypath=pypath, celery=self.app.conf['CELERY_BROKER'], proj_home=self.app.conf['PROJ_HOME'])
        if os.path.exists(pypath + '/activate'):
            command = 'source {pypath}/activate && '.format(pypath=pypath) + command 
            
        print 'executing', command
        subprocess.call(command, shell=True, executable='/bin/bash')
        
        # delete the vhost
        subprocess.check_call('docker exec {} rabbitmqctl delete_vhost {}'
                              .format(cid, vhost), shell=True)




    def test_functionality_of_the_system_on_non_existent_files(self):
        """
        Main test, it makes the all.links file, runs the injection of the
        bibcodes to the RabbitMQ instance using the run module, then waits
        for the articles full text to be extracted. Finally, it deletes the old
        files and folders.

        :return: no return
        """

        full_text_links = \
            'tests/test_functional/stub_data/fulltext_functional_tests.links'

        # Obtain the parameters to publish to the queue
        # Expect that the records are split into the correct number of
        # packet sizes
        self.helper_make_link(outfile=full_text_links)
        run.run(full_text_links=os.path.join(self.app.conf['PROJ_HOME'], full_text_links),
                packet_size=10,
                force_extract=False)

        time.sleep(40)

        self.expected_folders = self.calculate_expected_folders(full_text_links)

        for expected_f in self.expected_folders:
            self.assertTrue(
                os.path.exists(expected_f),
                'Could not find: {0}'
                .format(expected_f)
            )
            self.assertTrue(
                os.path.exists(os.path.join(expected_f, 'meta.json')),
                'Could not find: {0}'
                .format(os.path.join(expected_f, 'meta.json'))
            )
            self.assertTrue(
                os.path.exists(os.path.join(expected_f, 'fulltext.txt')),
                'Could not find: {0}'
                .format(os.path.join(expected_f, 'fulltext.txt'))
            )


if __name__ == '__main__':
    unittest.main()

