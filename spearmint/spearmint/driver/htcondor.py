import os
import sys
import re
import subprocess

from dispatch import DispatchDriver
from helpers  import *


# TODO: figure out if these modules are necessary, or if they can be handled in
# the matlab runner or a user script...

# System dependent modules
# Note these are specific to the Harvard configuration
DEFAULT_MODULES = [ 'packages/epd/7.1-2',
                    'packages/matlab/r2011b',
                    'mpi/openmpi/1.2.8/intel',
                    'libraries/mkl/10.0',
                    'packages/cuda/4.0',
                    ]

# Removed from SGE script...
# Load matlab modules
#module load %s

class HTCondorDriver(DispatchDriver):

    def submit_job(self, job):
        output_file = job_output_file(job)
        job_file    = job_file_for(job)
        modules     = " ".join(DEFAULT_MODULES)
        mint_path   = sys.argv[0]
        sge_script  = 'python %s --run-job "%s" .' % (mint_path, job_file)

        condor_file_template = """
Universe       = vanilla
Executable     = /software/python-2.7.10/bin/python

name = "%s-%d"

output  = %s 
error   = %s

getenv = True

arguments = %s --run-job \\"%s\\" .
 
Queue
"""
        
        condor_submit_script = condor_file_template % (job.name, job.id, output_file, output_file, mint_path, job_file)
        
        condor_submit_command = ["/usr/bin/condor_submit","-terse"]
        
        process = subprocess.Popen(" ".join(condor_submit_command),
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   shell=True)
        output = process.communicate(input=condor_submit_script)[0]
        process.stdin.close()
	
        # Parse out the job id.
        match = re.search(r'(\d+).(\d+)\s+-\s+(\d+).(\d+)', output)

        if match:
            return int(match.group(1))
        else:
            return None, output

    def is_proc_alive(self, job_id, sgeid):
        try:
            #query the job status
            process = subprocess.Popen(" ".join(["condor_q",str(sgeid)]),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   shell=True)
            output = process.communicate()[0]
            
            outputlines = output.split("\n")
            while not outputlines[0].strip().startswith("ID"):
                outputlines.pop(0)
            outputlines.pop(0)
            
            
            find_job_status_match = re.search(r'(\d+.\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)', outputlines[0])
            
            job_ST = None
            if find_job_status_match:
                job_ST = find_job_status_match.group(6)
            else:
                return False
            if job_ST in ['R','I','S']:
                return True
            return False
	except Exception as e:
	    raise e#just rethrow. Might add a finally later            

def init():
    return HTCondorDriver()

