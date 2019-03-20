import datetime, threading, time

class TimerThread:

	def __init__(self, nSecondsBetweenCall):
		self.thread = threading.Thread(target=self.Loop)
		self.nSecondsBetweenCall = nSecondsBetweenCall
		self.keepRunning = False

	def Loop(self):
		nextCall = time.time()
		while self.keepRunning:
			nextCall = nextCall + self.nSecondsBetweenCall
			self.Target()
			wait = nextCall - time.time()
			if wait > 0:
				time.sleep(wait)

	def Target(self):
		print (datetime.datetime.now())

	def Start(self):
		self.keepRunning = True
		self.thread.start()

if __name__ == '__main__':
	timerThread = TimerThread(1)
	timerThread.Start()
