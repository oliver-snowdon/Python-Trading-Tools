import os
import os.path

def InstallService(name, workingDirectory, command):
	fout = open('/etc/systemd/system/{}.service'.format(name), 'w')
	fout.write('[Service]\n')
	fout.write('User=root\n')
	fout.write('WorkingDirectory={}\n'.format(workingDirectory))
	fout.write('ExecStart={}\n'.format(command))
	fout.write('SuccessExitStatus=143\n')
	fout.write('TimeoutStopSec=10\n')
	fout.write('Restart=on-failure\n')
	fout.write('RestartSec=5\n')
	fout.write('[Install]\n')
	fout.write('WantedBy=multi-user.target\n')
	fout.flush()

	os.system('systemctl daemon-reload')
	os.system('systemctl enable {}.service'.format(name))
	os.system('systemctl start {}'.format(name))
	os.system('systemctl status {}'.format(name))
	
thisFile = os.path.realpath(__file__)
directory = os.path.dirname(thisFile)

InstallService('trade-logger', directory, os.path.join(directory, 'RunLogger.sh'))
InstallService('trade-server', directory, os.path.join(directory, 'RunServer.sh'))