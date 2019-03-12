import os
import os.path
thisFile = os.path.realpath(__file__)
directory = os.path.dirname(thisFile)

fout = open('/etc/systemd/system/trade-logger.service', 'w')
fout.write('[Service]\n')
fout.write('User=root\n')
fout.write('WorkingDirectory={}\n'.format(directory))
fout.write('ExecStart={}\n'.format(os.path.join(directory, 'RunLogger.sh')))
fout.write('SuccessExitStatus=143\n')
fout.write('TimeoutStopSec=10\n')
fout.write('Restart=on-failure\n')
fout.write('RestartSec=5\n')
fout.write('[Install]\n')
fout.write('WantedBy=multi-user.target\n')
fout.flush()

os.system('systemctl daemon-reload')
os.system('systemctl enable trade-logger.service')
os.system('systemctl start trade-logger')
os.system('systemctl status trade-logger')
