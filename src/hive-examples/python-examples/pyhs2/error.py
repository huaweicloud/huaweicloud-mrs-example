class Pyhs2Exception(Exception):
	def __init__(self, errorCode, errorMessage):
		self.errorCode = errorCode
		self.errorMessage = errorMessage
	def __str__(self):
		return repr(self.errorMessage)