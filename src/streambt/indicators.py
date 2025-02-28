"""
the user need to be able to instantiate an indicator like 
RSI(backend=sparkbackend,feed=feed, other params)

this rsi ideally would be dynamically created here such that it 
auto configure it to corresponding talib function from respective backend if possible   
"""
class Indicator:
    def __init__(self, backend, feed):
        self.backend = backend
        self.feed = feed